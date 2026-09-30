# ruff: noqa: INP001
"""The recommender's ranking stage: a CatBoost classifier over customer and article features.

Run as the job `<slug>-train-ranker` in `<slug>-jobs-env`, after `<slug>-features`:

    hops job deploy <slug>-train-ranker src/<slug_pkg>/train_ranker.py \
        --env <slug>-jobs-env --run --wait

As in the course: every purchase is a positive pair, ten random (customer,
article) pairs per purchase are negatives, and each pair carries the customer's
age and the article's categorical attributes. The classifier's probability of
a purchase ranks the candidates retrieval returns. It is registered as
`ranking_model`, with precision, recall, F1 and ROC-AUC on a held-out tenth,
and `features.json` naming its inputs in order.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import polars as pl

CATEGORICAL = [
    "product_type_name",
    "product_group_name",
    "graphical_appearance_name",
    "colour_group_name",
    "perceived_colour_value_name",
    "perceived_colour_master_name",
    "department_name",
    "index_name",
    "index_group_name",
    "section_name",
    "garment_group_name",
]
FEATURES = ["age", *CATEGORICAL]
NEGATIVES_PER_PURCHASE = 10


def ranking_pairs(
    purchases: pl.DataFrame, customers: pl.DataFrame, articles: pl.DataFrame, seed: int = 27
) -> pl.DataFrame:
    """Labelled (customer, article) pairs with the ranking features."""
    rng = np.random.default_rng(seed)
    positives = purchases.select("customer_id", "article_id").unique().with_columns(label=pl.lit(1))
    n = positives.height * NEGATIVES_PER_PURCHASE
    negatives = (
        pl.DataFrame(
            {
                "customer_id": rng.choice(customers["customer_id"].to_numpy(), n),
                "article_id": rng.choice(articles["article_id"].to_numpy(), n),
            }
        )
        .join(positives, on=["customer_id", "article_id"], how="anti")
        .with_columns(label=pl.lit(0))
    )
    return (
        pl.concat([positives, negatives])
        .join(customers.select("customer_id", "age"), on="customer_id")
        .join(articles.select("article_id", *CATEGORICAL), on="article_id")
        .select(*FEATURES, "label")
    )


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float:
    """The probability a random positive outscores a random negative, ties counting half."""
    order = scores.argsort()
    ranks = np.empty(len(scores))
    ranks[order] = np.arange(1, len(scores) + 1)
    for value in np.unique(scores):
        tied = scores == value
        ranks[tied] = ranks[tied].mean()
    positives = labels == 1
    n_pos, n_neg = positives.sum(), (~positives).sum()
    return float((ranks[positives].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def evaluate(labels: np.ndarray, scores: np.ndarray, threshold: float = 0.5) -> dict:
    predicted = scores >= threshold
    tp = float(np.sum(predicted & (labels == 1)))
    precision = tp / max(float(predicted.sum()), 1.0)
    recall = tp / max(float((labels == 1).sum()), 1.0)
    f1 = 2 * precision * recall / max(precision + recall, 1e-12)
    return {"precision": precision, "recall": recall, "f1": f1, "roc_auc": roc_auc(labels, scores)}


def train(pairs: pl.DataFrame, seed: int = 27):
    from catboost import CatBoostClassifier, Pool

    rng = np.random.default_rng(seed)
    held_out = pl.Series(rng.random(pairs.height) < 0.1)
    train_df, test_df = pairs.filter(~held_out), pairs.filter(held_out)

    def pool(df: pl.DataFrame):
        return Pool(
            df.select(FEATURES).to_pandas(), df["label"].to_numpy(), cat_features=CATEGORICAL
        )

    model = CatBoostClassifier(
        learning_rate=0.2,
        iterations=100,
        depth=10,
        scale_pos_weight=NEGATIVES_PER_PURCHASE,
        early_stopping_rounds=5,
        use_best_model=True,
        random_seed=seed,
        verbose=False,
        allow_writing_files=False,
    )
    model.fit(pool(train_df), eval_set=pool(test_df))
    scores = model.predict_proba(test_df.select(FEATURES).to_pandas())[:, 1]
    return model, evaluate(test_df["label"].to_numpy(), scores)


def main() -> int:
    import hopsworks

    project = hopsworks.login()
    fs = project.get_feature_store()
    purchases = (
        fs.get_feature_group("transactions", version=1)
        .select(["customer_id", "article_id"])
        .read(dataframe_type="polars")
    )
    customers = (
        fs.get_feature_group("customers", version=1)
        .select(["customer_id", "age"])
        .read(dataframe_type="polars")
    )
    articles = (
        fs.get_feature_group("articles", version=1)
        .select(["article_id", *CATEGORICAL])
        .read(dataframe_type="polars")
    )

    model, metrics = train(ranking_pairs(purchases, customers, articles))
    print("ranking metrics:", json.dumps(metrics))
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        model.save_model(str(directory / "ranking_model.cbm"))
        (directory / "features.json").write_text(
            json.dumps({"features": FEATURES, "categorical": CATEGORICAL})
        )
        registered = project.get_model_registry().python.create_model(
            name="ranking_model",
            metrics=metrics,
            description="CatBoost purchase probability of a customer for a candidate article",
        )
        registered.save(str(directory))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

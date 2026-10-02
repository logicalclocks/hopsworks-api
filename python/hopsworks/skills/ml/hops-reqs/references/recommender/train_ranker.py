# ruff: noqa: INP001
"""The recommender's ranking stage: a CatBoost classifier over customer and article features.

Run as the job `<slug>-train-ranker` in `<slug>-jobs-env`, after `<slug>-features`:

    hops job deploy <slug>-train-ranker src/<slug_pkg>/train_ranker.py \
        --env <slug>-jobs-env --run --wait

Each customer's latest purchases are positive pairs and ten other articles per
purchase, drawn by popularity, are negatives. Each pair carries the customer's
age, the article's categorical attributes, and the customer's taste: the share
of their earlier purchases with the article's colour, index group, garment
group, product type and section. The classifier's probability of a purchase
ranks the candidates retrieval returns. It is registered as
`ranking_model`, with precision, recall, F1 and ROC-AUC on a held-out tenth,
and `features.json` naming its inputs in order.
"""

from __future__ import annotations

import json
import os
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
# The attributes whose share in the customer's own purchases is a feature: without
# them the model sees only the article and the customer's age, so it ranks the
# same popular articles first for everyone.
TASTE = [
    "colour_group_name",
    "index_group_name",
    "garment_group_name",
    "product_type_name",
    "section_name",
]
FEATURES = ["age", *CATEGORICAL, *(f"{column}_share" for column in TASTE)]
NEGATIVES_PER_PURCHASE = 10
# 1.1 million labelled pairs: what a 2 GB job holds with room for CatBoost.
MAX_PURCHASES = 100_000


def cpu_limit() -> int:
    """The container's CPU limit, from cgroup v2's cpu.max; the visible cores without one.

    torch and CatBoost start a thread per core of the node, and sixteen threads
    under a one-CPU limit are throttled to a crawl.
    """
    try:
        quota, period = open("/sys/fs/cgroup/cpu.max").read().split()  # noqa: PTH123, SIM115
        if quota != "max":
            return max(1, int(quota) // int(period))
    except (OSError, ValueError):
        pass
    return os.cpu_count() or 1


def split_history(purchases: pl.DataFrame, label_share: float = 0.2) -> tuple:
    """Each customer's earlier purchases as history and the latest `label_share` as labels.

    A customer with a single purchase has no history to learn a taste from and is
    left out. The split is what keeps the taste features honest: a purchase never
    counts towards its own pair's shares.
    """
    ranked = (
        purchases.filter(pl.len().over("customer_id") > 1)
        .sort("t_dat")
        .with_columns(
            position=pl.int_range(pl.len()).over("customer_id"),
            n=pl.len().over("customer_id"),
        )
    )
    in_labels = pl.col("position") >= (pl.col("n") * (1 - label_share)).floor()
    return ranked.filter(~in_labels), ranked.filter(in_labels)


def taste(history: pl.DataFrame, pairs: pl.DataFrame) -> pl.DataFrame:
    """`pairs` with, per attribute, the share of the customer's history sharing the article's.

    Both frames carry customer_id and the TASTE attributes; a customer without
    history gets shares of 0.
    """
    for column in TASTE:
        shares = (
            history.group_by("customer_id", column)
            .len()
            .with_columns(
                (pl.col("len") / pl.col("len").sum().over("customer_id")).alias(f"{column}_share")
            )
        )
        pairs = pairs.join(
            shares.select("customer_id", column, f"{column}_share"),
            on=["customer_id", column],
            how="left",
        ).with_columns(pl.col(f"{column}_share").fill_null(0.0))
    return pairs


def ranking_pairs(
    purchases: pl.DataFrame, customers: pl.DataFrame, articles: pl.DataFrame, seed: int = 27
) -> pl.DataFrame:
    """Labelled (customer, article) pairs with the ranking features.

    The positives are each customer's latest purchases and the negatives other
    articles for the same customers, drawn by popularity, as retrieval's
    candidates are. Uniform negatives are mostly articles nobody buys, which
    teaches the model that popular colours sell, and it then ranks black first for
    every customer.
    """
    rng = np.random.default_rng(seed)
    history, labels = split_history(purchases)
    positives = labels.select("customer_id", "article_id").unique(maintain_order=True)
    if positives.height > MAX_PURCHASES:
        positives = positives.sample(MAX_PURCHASES, seed=seed)
    positives = positives.with_columns(label=pl.lit(1))
    n = positives.height * NEGATIVES_PER_PURCHASE
    negatives = (
        pl.DataFrame(
            {
                "customer_id": np.repeat(
                    positives["customer_id"].to_numpy(), NEGATIVES_PER_PURCHASE
                ),
                "article_id": rng.choice(purchases["article_id"].to_numpy(), n),
            }
        )
        .join(
            purchases.select("customer_id", "article_id"),
            on=["customer_id", "article_id"],
            how="anti",
        )
        .with_columns(label=pl.lit(0))
    )
    attributes = articles.select("article_id", *CATEGORICAL)
    pairs = (
        pl.concat([positives, negatives])
        .join(customers.select("customer_id", "age"), on="customer_id")
        .join(attributes, on="article_id")
    )
    return taste(history.join(attributes, on="article_id"), pairs).select(*FEATURES, "label")


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

    def features(df: pl.DataFrame):
        # Categories, not Python strings: a million rows of eleven string columns
        # as pandas objects is gigabytes.
        return (
            df.select(FEATURES).with_columns(pl.col(CATEGORICAL).cast(pl.Categorical)).to_pandas()
        )

    def pool(df: pl.DataFrame):
        return Pool(features(df), df["label"].to_numpy(), cat_features=CATEGORICAL)

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
        thread_count=cpu_limit(),
    )
    model.fit(pool(train_df), eval_set=pool(test_df))
    scores = model.predict_proba(features(test_df))[:, 1]
    return model, evaluate(test_df["label"].to_numpy(), scores)


def main() -> int:
    import hopsworks

    project = hopsworks.login()
    fs = project.get_feature_store()
    purchases = (
        fs.get_feature_group("transactions", version=1)
        .select(["customer_id", "article_id", "t_dat"])
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
            json.dumps({"features": FEATURES, "categorical": CATEGORICAL, "taste": TASTE})
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

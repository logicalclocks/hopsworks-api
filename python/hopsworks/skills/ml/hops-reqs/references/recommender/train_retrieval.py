# ruff: noqa: INP001
"""The recommender's retrieval stage: a two-tower model and the candidate embeddings.

Run as the job `<slug>-train-retrieval` in `<slug>-jobs-env`, after `<slug>-features`:

    hops job deploy <slug>-train-retrieval src/<slug_pkg>/train_retrieval.py \
        --env <slug>-jobs-env --run --wait

The course's two-tower model, in PyTorch on the CPU. A query tower (customer id,
age, month) and an item tower (article id, garment group, index group) map into
one 16-dimensional space, trained with an in-batch softmax loss. It
creates the feature views the deployment reads (`retrieval`, `customers`,
`articles`), registers the query tower as `query_model` with its recall@100 on
the test split, and writes every article's embedding from the item tower into
`candidate_embeddings`, whose vector index the deployment searches, with a second,
centered embedding per article for the session and the `session_embeddings`
feature view over it. Recall is measured on the real purchases of the test split
only.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

import numpy as np
import polars as pl

EMBEDDING_SIZE = 16
ITEM_FEATURES = ["article_id", "garment_group_name", "index_group_name"]
SERVED_ARTICLE_FEATURES = [
    "article_id",
    "prod_name",
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
    "image_url",
]


def cpu_limit() -> int:
    """The container's CPU limit, from cgroup v2's cpu.max; the visible cores without one.

    torch and CatBoost start a thread per core of the node, and sixteen threads
    under a one-CPU limit are throttled to a crawl.
    """
    try:
        quota, period = open("/sys/fs/cgroup/cpu.max", encoding="utf-8").read().split()  # noqa: PTH123, SIM115
        if quota != "max":
            return max(1, int(quota) // int(period))
    except (OSError, ValueError):
        pass
    return os.cpu_count() or 1


def vocabulary(values: pl.Series) -> dict[str, int]:
    """Index 0 is for values the model has not seen."""
    return {value: i + 1 for i, value in enumerate(sorted(values.unique().to_list()))}


def encode(values: pl.Series, vocab: dict[str, int]) -> np.ndarray:
    return values.replace_strict(vocab, default=0, return_dtype=pl.Int64).to_numpy(writable=True)


def build_towers(n_customers: int, n_articles: int, n_garments: int, n_indexes: int):
    """The query and item towers; both end in an EMBEDDING_SIZE projection."""
    import torch
    from torch import nn

    class QueryTower(nn.Module):
        def __init__(self, age_mean: float = 0.0, age_std: float = 1.0) -> None:
            super().__init__()
            self.customer = nn.Embedding(n_customers + 1, EMBEDDING_SIZE)
            self.register_buffer("age_stats", torch.tensor([age_mean, age_std]))
            self.fnn = nn.Sequential(
                nn.Linear(EMBEDDING_SIZE + 3, EMBEDDING_SIZE),
                nn.ReLU(),
                nn.Linear(EMBEDDING_SIZE, EMBEDDING_SIZE),
            )

        def forward(self, customer, age, month_sin, month_cos):
            age = (age - self.age_stats[0]) / self.age_stats[1]
            numeric = torch.stack([age, month_sin, month_cos], dim=1)
            return self.fnn(torch.cat([self.customer(customer), numeric], dim=1))

    class ItemTower(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.article = nn.Embedding(n_articles + 1, EMBEDDING_SIZE)
            self.n_garments, self.n_indexes = n_garments + 1, n_indexes + 1
            self.fnn = nn.Sequential(
                nn.Linear(EMBEDDING_SIZE + self.n_garments + self.n_indexes, EMBEDDING_SIZE),
                nn.ReLU(),
                nn.Linear(EMBEDDING_SIZE, EMBEDDING_SIZE),
            )

        def forward(self, article, garment, index):
            garments = nn.functional.one_hot(garment, self.n_garments).float()
            indexes = nn.functional.one_hot(index, self.n_indexes).float()
            return self.fnn(torch.cat([self.article(article), garments, indexes], dim=1))

    return QueryTower, ItemTower


def recall_at_k(
    query: np.ndarray, items: np.ndarray, true_items: np.ndarray, k: int = 100, chunk: int = 1024
) -> float:
    """The share of test purchases whose article is among the k best-scoring items.

    A purchase hits when fewer than k items outscore its article. Scored a chunk
    of queries at a time: all of them against every article is gigabytes.
    """
    hits = 0
    for start in range(0, len(query), chunk):
        scores = query[start : start + chunk] @ items.T
        true = true_items[start : start + chunk]
        known = true >= 0
        own = scores[np.arange(len(true)), np.where(known, true, 0)]
        hits += int(np.sum(known & ((scores > own[:, None]).sum(axis=1) < k)))
    return hits / max(len(query), 1)


def train(frames: dict, epochs: int = 20, batch_size: int = 512, lr: float = 0.01, seed: int = 27):
    """Train the towers on `frames["train"]`; returns them, the vocabularies and test recall."""
    import torch

    torch.manual_seed(seed)
    torch.set_num_threads(cpu_limit())
    train_df, test_df = frames["train"], frames["test"]
    vocabs = {
        "customer_id": vocabulary(train_df["customer_id"]),
        "article_id": vocabulary(train_df["article_id"]),
        "garment_group_name": vocabulary(train_df["garment_group_name"]),
        "index_group_name": vocabulary(train_df["index_group_name"]),
    }
    QueryTower, ItemTower = build_towers(*(len(vocabs[c]) for c in vocabs))
    ages = train_df["age"].to_numpy()
    query_tower = QueryTower(float(ages.mean()), float(ages.std() or 1.0))
    item_tower = ItemTower()

    def tensors(df: pl.DataFrame):
        return (
            torch.tensor(encode(df["customer_id"], vocabs["customer_id"])),
            torch.tensor(df["age"].to_numpy(), dtype=torch.float32),
            torch.tensor(df["month_sin"].to_numpy(), dtype=torch.float32),
            torch.tensor(df["month_cos"].to_numpy(), dtype=torch.float32),
            torch.tensor(encode(df["article_id"], vocabs["article_id"])),
            torch.tensor(encode(df["garment_group_name"], vocabs["garment_group_name"])),
            torch.tensor(encode(df["index_group_name"], vocabs["index_group_name"])),
        )

    columns = tensors(train_df)
    # How often each article is a batch's positive: in-batch negatives over-sample
    # popular articles, and subtracting log(frequency) from the logits corrects it
    # (Yi et al., "Sampling-bias-corrected neural modeling", 2019).
    counts = torch.bincount(columns[4], minlength=len(vocabs["article_id"]) + 1).float()
    log_q = torch.log(counts / counts.sum() + 1e-12)
    params = list(query_tower.parameters()) + list(item_tower.parameters())
    optimizer = torch.optim.AdamW(params, lr=lr, weight_decay=0.001)
    for _ in range(epochs):
        order = torch.randperm(train_df.height)
        for start in range(0, train_df.height, batch_size):
            batch = [c[order[start : start + batch_size]] for c in columns]
            queries = query_tower(*batch[:4])
            items = item_tower(*batch[4:])
            # In-batch softmax: each query's own purchase against the batch's other items.
            logits = queries @ items.T - log_q[batch[4]]
            loss = torch.nn.functional.cross_entropy(logits, torch.arange(len(logits)))
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

    query_tower.eval()
    item_tower.eval()
    catalogue = train_df.select(ITEM_FEATURES).unique("article_id").sort("article_id")
    with torch.no_grad():
        item_vectors = embed_items(item_tower, catalogue, vocabs)
        test = tensors(test_df)
        query_vectors = query_tower(*test[:4]).numpy()
    position = {a: i for i, a in enumerate(catalogue["article_id"].to_list())}
    true_items = np.array([position.get(a, -1) for a in test_df["article_id"].to_list()])
    recall = recall_at_k(query_vectors, item_vectors, true_items)
    return query_tower, item_tower, vocabs, recall


def embed_items(item_tower, articles: pl.DataFrame, vocabs: dict) -> np.ndarray:
    import torch

    with torch.no_grad():
        return item_tower(
            torch.tensor(encode(articles["article_id"], vocabs["article_id"])),
            torch.tensor(encode(articles["garment_group_name"], vocabs["garment_group_name"])),
            torch.tensor(encode(articles["index_group_name"], vocabs["index_group_name"])),
        ).numpy()


def session_embeddings(vectors: np.ndarray) -> np.ndarray:
    """The item embeddings with the catalogue's mean taken out, at unit length.

    Every item embedding shares one large direction (any two have a cosine near
    1.0), and only what is left tells a shoe from a sweater: a shoe is about 0.5
    like other shoes and 0.06 like the rest. These are what the deployment
    compares a shopper's session with, through their own cosine index.
    """
    centered = vectors - vectors.mean(axis=0)
    return centered / np.linalg.norm(centered, axis=1, keepdims=True)


def save_query_model(query_tower, vocabs: dict, directory: Path) -> Path:
    """TorchScript, so the deployment runs it without this file's class definitions."""
    import torch

    torch.jit.script(query_tower).save(str(directory / "query_tower.pt"))
    (directory / "customer_vocab.json").write_text(
        json.dumps(vocabs["customer_id"]), encoding="utf-8"
    )
    return directory


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the two-tower retrieval model.")
    parser.add_argument("--epochs", type=int, default=20)
    args = parser.parse_args(argv)

    import hopsworks
    from hsfs import embedding

    project = hopsworks.login()
    fs = project.get_feature_store()
    transactions = fs.get_feature_group("transactions", version=1)
    customers = fs.get_feature_group("customers", version=1)
    articles = fs.get_feature_group("articles", version=1)

    retrieval = fs.get_or_create_feature_view(
        name="retrieval",
        version=1,
        query=transactions.select(
            ["customer_id", "article_id", "t_dat", "month_sin", "month_cos", "synthetic"]
        )
        .join(customers.select(["age"]), on="customer_id")
        .join(articles.select(["garment_group_name", "index_group_name"]), on="article_id"),
    )
    fs.get_or_create_feature_view(name="customers", version=1, query=customers.select_all())
    fs.get_or_create_feature_view(
        name="articles", version=1, query=articles.select(SERVED_ARTICLE_FEATURES)
    )

    x_train, _, x_test, _, _, _ = retrieval.train_validation_test_split(
        validation_size=0.1, test_size=0.1, statistics_config=False
    )
    # Recall is measured on real purchases: the synthetic ones follow the model's own
    # assumption about taste, so scoring them would flatter it.
    test = pl.from_pandas(x_test).filter(~pl.col("synthetic"))
    frames = {"train": pl.from_pandas(x_train), "test": test}
    query_tower, item_tower, vocabs, recall = train(frames, epochs=args.epochs)
    print(f"recall@100 on the test split: {recall:.3f}")

    # The item tower embeds only the articles it was trained on; any other would
    # share the unknown-id embedding and crowd every customer's neighbours.
    catalogue = (
        articles.select(ITEM_FEATURES)
        .read(dataframe_type="polars")
        .filter(pl.col("article_id").is_in(list(vocabs["article_id"])))
    )
    vectors = embed_items(item_tower, catalogue, vocabs)
    candidates = catalogue.select("article_id").with_columns(
        embeddings=pl.Series(vectors.tolist()),
        session_embedding=pl.Series(session_embeddings(vectors).tolist()),
    )

    with tempfile.TemporaryDirectory() as tmp:
        directory = save_query_model(query_tower, vocabs, Path(tmp))
        model = project.get_model_registry().torch.create_model(
            name="query_model",
            metrics={"recall_at_100": recall},
            description="Two-tower query tower: customer id, age and month to a 16-d embedding",
            feature_view=retrieval,
        )
        model.save(str(directory))

    index = embedding.EmbeddingIndex()
    # The towers are trained on dot products; the index's default, L2 distance, ranks
    # the customer's least likely articles first for unnormalised vectors.
    index.add_embedding("embeddings", EMBEDDING_SIZE, embedding.SimilarityFunctionType.DOT_PRODUCT)
    # The session's own index: cosine over the centered vectors, where a centered
    # query lies among the indexed vectors. Searched with an off-distribution query,
    # the dot-product index's approximate search returns poor neighbours.
    index.add_embedding(
        "session_embedding", EMBEDDING_SIZE, embedding.SimilarityFunctionType.COSINE
    )
    fg = fs.get_or_create_feature_group(
        name="candidate_embeddings",
        version=1,
        primary_key=["article_id"],
        online_enabled=True,
        embedding_index=index,
        description="Two-tower item embeddings of every article, searched by the deployment",
        statistics_config=False,
    )
    fg.insert(candidates.to_pandas(), write_options={"wait_for_job": True})
    # The deployment looks up the session vectors of the articles a shopper clicked.
    fs.get_or_create_feature_view(
        name="session_embeddings",
        version=1,
        query=fg.select(["article_id", "session_embedding"]),
    )
    print(f"candidate_embeddings: {candidates.height} articles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# ruff: noqa: INP001
"""The recommender's feature pipeline: H&M customers, articles, transactions and interactions.

Run as the job `<slug>-features` in `<slug>-jobs-env`:

    hops job deploy <slug>-features src/<slug_pkg>/hm_features.py --env <slug>-jobs-env \
        --args "--customers 5000 --synthetic 30" --run --wait

Based on the Decoding AI / Hopsworks H&M course
(https://github.com/decodingai-magazine/personalized-recommender-course). It reads
the public H&M files, samples customers, and writes four online feature groups:

- `customers`: customer_id, club_member_status, age, postal_code, age_group;
- `articles`: the attributes, a text description and the image URL of every
  article the sampled customers bought, the catalogue the models know;
- `transactions`: the sampled customers' purchases, with month_sin and month_cos,
  and `--synthetic` more per customer in the shape of their own (`synthetic`
  true), because the real sample is too sparse for the two-tower model;
- `interactions`: the purchases plus clicks and ignores generated around them
  (interaction_score 2, 1 and 0), which the app appends to as shoppers use it.

The transactions file is 3.5 GB, so it is streamed and only the sampled
customers' lines are kept. Every column is built whole, with seeded numpy draws
and Polars expressions.
"""

from __future__ import annotations

import argparse
import math
import urllib.request

import numpy as np
import polars as pl

H_AND_M = "https://repo.hops.works/dev/jdowling/h-and-m"
SEED = 27

# The deployment and the app read a customer's purchases and interactions online by
# customer_id alone. The online key leads with the event time, so without an index
# on customer_id that read scans the table: 220 ms at 250,000 rows.
BY_CUSTOMER = {"secondary_indexes": [["customer_id"]]}

ARTICLE_COLUMNS = [
    "article_id",
    "product_code",
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
]


# region Customers


def age_group() -> pl.Expr:
    """The customer's age band, as the course's retrieval features use it."""
    age = pl.col("age")
    return (
        pl.when(age <= 18)
        .then(pl.lit("0-18"))
        .when(age <= 25)
        .then(pl.lit("19-25"))
        .when(age <= 35)
        .then(pl.lit("26-35"))
        .when(age <= 45)
        .then(pl.lit("36-45"))
        .when(age <= 55)
        .then(pl.lit("46-55"))
        .when(age <= 65)
        .then(pl.lit("56-65"))
        .otherwise(pl.lit("66+"))
        .alias("age_group")
    )


def compute_customers(customers: pl.DataFrame) -> pl.DataFrame:
    """Customers with an age, their club status filled in and an age band."""
    return (
        customers.drop_nulls("age")
        .with_columns(
            pl.col("club_member_status").fill_null("ABSENT"),
            pl.col("age").cast(pl.Float64),
        )
        .with_columns(age_group())
        .select(["customer_id", "club_member_status", "age", "postal_code", "age_group"])
    )


# endregion

# region Articles


def image_url() -> pl.Expr:
    """The article's picture: images/0<first two digits>/0<article_id>.jpg."""
    article = pl.col("article_id")
    return pl.format(f"{H_AND_M}/images/0{{}}/0{{}}.jpg", article.str.slice(0, 2), article).alias(
        "image_url"
    )


def compute_articles(articles: pl.DataFrame) -> pl.DataFrame:
    """Articles with an id as text, a description to embed and an image URL."""
    articles = articles.with_columns(
        pl.col("article_id").cast(pl.Utf8).str.strip_chars_start("0"),
        pl.col("detail_desc").fill_null(""),
    )
    description = pl.concat_str(
        [
            pl.col("prod_name"),
            pl.lit(" - "),
            pl.col("product_type_name"),
            pl.lit(" in "),
            pl.col("product_group_name"),
            pl.lit("\nAppearance: "),
            pl.col("graphical_appearance_name"),
            pl.lit("\nColor: "),
            pl.col("perceived_colour_value_name"),
            pl.lit(" "),
            pl.col("perceived_colour_master_name"),
            pl.lit(" ("),
            pl.col("colour_group_name"),
            pl.lit(")\nCategory: "),
            pl.col("index_group_name"),
            pl.lit(" - "),
            pl.col("section_name"),
            pl.lit(" - "),
            pl.col("garment_group_name"),
            pl.when(pl.col("detail_desc") != "")
            .then(pl.lit("\nDetails: ") + pl.col("detail_desc"))
            .otherwise(pl.lit("")),
        ]
    ).alias("article_description")
    return articles.select(
        *ARTICLE_COLUMNS,
        description,
        prod_name_length=pl.col("prod_name").str.len_chars(),
    ).with_columns(image_url())


# endregion

# region Transactions and interactions


def compute_transactions(transactions: pl.DataFrame) -> pl.DataFrame:
    """Purchases with the article id as the articles group keys it, and the month's cycle."""
    month = pl.col("t_dat").dt.month()
    return (
        transactions.with_columns(
            pl.col("article_id").cast(pl.Utf8).str.strip_chars_start("0"),
            pl.col("t_dat").cast(pl.Datetime("us")),
            month_sin=(month * (2 * math.pi / 12)).sin(),
            month_cos=(month * (2 * math.pi / 12)).cos(),
        )
        # Two of the same article on one day is one row: the primary key.
        .unique(["customer_id", "article_id", "t_dat"], keep="first", maintain_order=True)
        .select(
            [
                "t_dat",
                "customer_id",
                "article_id",
                "price",
                "sales_channel_id",
                "month_sin",
                "month_cos",
            ]
        )
    )


def read_purchases(url: str, customer_ids: list[str]) -> pl.DataFrame:
    """The lines of the transactions file whose customer is one of `customer_ids`.

    A line is `t_dat,customer_id,...` with a 10-character date, so the customer
    id is bytes 11 to 75; comparing those avoids parsing 31 million lines.
    """
    wanted = {c.encode() for c in customer_ids}
    with urllib.request.urlopen(url) as response:  # noqa: S310 - the fixed H&M mirror
        header = response.readline()
        kept = [line for line in response if line[11:75] in wanted]
    return pl.read_csv(header + b"".join(kept), try_parse_dates=True)


def synthetic_purchases(
    transactions: pl.DataFrame, articles: pl.DataFrame, per_customer: int, seed: int = SEED
) -> pl.DataFrame:
    """`per_customer` more purchases per customer, in the shape of the ones they made.

    Each copies one of the customer's real purchases, drawn uniformly, so a group
    they buy often is drawn often; swaps its article for one of the same index and
    garment group, drawn by popularity; and moves it to a random day between the
    customer's first and last purchase. The sample of real customers is too sparse
    for a two-tower model to learn tastes from; these rows carry the same tastes,
    denser. They are marked `synthetic`.
    """
    rng = np.random.default_rng([seed, 1])
    groups = ["index_group_name", "garment_group_name"]
    bought = transactions.join(articles.select("article_id", *groups), on="article_id")
    counts = bought.group_by("customer_id").len()
    picks = pl.Series(rng.random(counts.height * per_customer))
    sampled = (
        counts.select(pl.all().repeat_by(per_customer).explode())
        .with_columns(row=(picks * pl.col("len")).floor().cast(pl.UInt32))
        .select("customer_id", "row")
        .join(
            bought.with_columns(row=pl.int_range(pl.len()).over("customer_id").cast(pl.UInt32)),
            on=["customer_id", "row"],
        )
    )
    span = bought.group_by("customer_id").agg(
        first=pl.col("t_dat").min(), last=pl.col("t_dat").max()
    )
    popularity = bought.group_by("article_id", *groups).len()
    parts = []
    for key, rows in sampled.group_by(groups):
        pool = popularity.filter((pl.col(groups[0]) == key[0]) & (pl.col(groups[1]) == key[1]))
        weights = pool["len"].to_numpy() / pool["len"].sum()
        parts.append(
            rows.with_columns(
                article_id=pl.Series(
                    rng.choice(pool["article_id"].to_numpy(), rows.height, p=weights)
                )
            )
        )
    days = pl.duration(days=pl.col("offset"))
    return (
        pl.concat(parts)
        .join(span, on="customer_id")
        .with_columns(u=pl.Series(rng.random(sampled.height)))
        .with_columns(
            offset=((pl.col("last") - pl.col("first")).dt.total_days() * pl.col("u")).floor()
        )
        .with_columns(t_dat=pl.col("first") + days)
        .pipe(
            lambda df: compute_transactions(
                df.select("t_dat", "customer_id", "article_id", "price", "sales_channel_id")
            )
        )
    )


def generate_interactions(
    transactions: pl.DataFrame, articles: pl.Series, seed: int = SEED
) -> pl.DataFrame:
    """Purchases (2), the clicks before them (1), and clicks and ignores (0) on other articles.

    As in the course: most purchases are preceded by one or two clicks within two
    days, and every customer ignores 40 to 60 articles and clicks 5 to 8 more in
    the days before their last purchase. `prev_article_id` is the article of the
    customer's previous interaction, START for the first.
    """
    rng = np.random.default_rng(seed)
    hour = pl.duration(hours=1)
    purchases = transactions.select(
        "t_dat", "customer_id", "article_id", interaction_score=pl.lit(2, pl.Int64)
    )
    clicked = purchases.filter(pl.Series(rng.random(purchases.height) < 0.9))
    before = clicked.with_columns(
        t_dat=pl.col("t_dat") - hour * pl.Series(rng.integers(1, 48, clicked.height)),
        interaction_score=pl.lit(1, pl.Int64),
    )
    last = transactions.group_by("customer_id").agg(last=pl.col("t_dat").max()).sort("customer_id")
    candidates = articles.to_numpy()

    def around_last(low: int, high: int, score: int, max_hours: int) -> pl.DataFrame:
        counts = rng.integers(low, high + 1, last.height)
        rows = int(counts.sum())
        return pl.DataFrame(
            {
                "customer_id": np.repeat(last["customer_id"].to_numpy(), counts),
                "last": np.repeat(last["last"].to_numpy(), counts),
                "article_id": rng.choice(candidates, rows),
                "hours": rng.integers(1, max_hours, rows),
            }
        ).select(
            t_dat=pl.col("last") - hour * pl.col("hours"),
            customer_id="customer_id",
            article_id="article_id",
            interaction_score=pl.lit(score, pl.Int64),
        )

    return (
        pl.concat([purchases, before, around_last(40, 60, 0, 96), around_last(5, 8, 1, 72)])
        .unique(
            ["customer_id", "article_id", "t_dat", "interaction_score"],
            keep="first",
            maintain_order=True,
        )
        .sort(["customer_id", "t_dat"], maintain_order=True)
        .with_columns(
            prev_article_id=pl.col("article_id").shift(1).over("customer_id").fill_null("START")
        )
    )


# endregion

# region The feature groups


def write(fs, df: pl.DataFrame, name: str, primary_key: list[str], **options):
    fg = fs.get_or_create_feature_group(
        name=name,
        version=1,
        primary_key=primary_key,
        online_enabled=True,
        description=options.pop("description"),
        statistics_config=False,
        **options,
    )
    fg.insert(df.to_pandas(), write_options={"wait_for_job": True})
    return fg


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write the H&M feature groups.")
    parser.add_argument("--customers", type=int, default=5000, help="customers sampled")
    parser.add_argument(
        "--synthetic", type=int, default=30, help="synthetic purchases added per customer"
    )
    # A scheduled run appends -start_time <fire instant> (and may append -end_time):
    # the instant the schedule fired, which ends the window rather than starting it.
    # They are accepted and ignored; a scheduled run's window is HOPS_START_TIME/HOPS_END_TIME.
    parser.add_argument("-start_time", dest="_fired_at", help=argparse.SUPPRESS)
    parser.add_argument("-end_time", dest="_fired_end", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    import hopsworks

    customers = compute_customers(pl.read_csv(f"{H_AND_M}/customers.csv")).sample(
        n=args.customers, seed=SEED
    )
    transactions = compute_transactions(
        read_purchases(f"{H_AND_M}/transactions_train.csv", customers["customer_id"].to_list())
    )
    # A customer without a purchase has nothing to train on or to show.
    customers = customers.filter(pl.col("customer_id").is_in(transactions["customer_id"].implode()))
    articles = compute_articles(pl.read_csv(f"{H_AND_M}/articles.csv", infer_schema_length=0))
    articles = articles.filter(pl.col("article_id").is_in(transactions["article_id"].implode()))
    real = transactions.with_columns(synthetic=pl.lit(False))
    if args.synthetic:
        extra = synthetic_purchases(transactions, articles, args.synthetic)
        # A drawn purchase that repeats a real one on the same day is the real one.
        extra = extra.join(real, on=["customer_id", "article_id", "t_dat"], how="anti")
        transactions = pl.concat([real, extra.with_columns(synthetic=pl.lit(True))])
        transactions = transactions.unique(
            ["customer_id", "article_id", "t_dat"], keep="first", maintain_order=True
        )
    else:
        transactions = real
    interactions = generate_interactions(transactions, articles["article_id"])

    fs = hopsworks.login().get_feature_store()
    write(
        fs, customers, "customers", ["customer_id"], description="H&M customers: age, club status"
    )
    write(
        fs,
        articles,
        "articles",
        ["article_id"],
        description="H&M articles: type, colour, category, description and image",
    )
    write(
        fs,
        transactions,
        "transactions",
        ["customer_id", "article_id", "t_dat"],
        event_time="t_dat",
        description="H&M purchases of the sampled customers; synthetic marks the generated ones",
        online_config=BY_CUSTOMER,
    )
    write(
        fs,
        interactions,
        "interactions",
        ["customer_id", "article_id", "t_dat", "interaction_score"],
        event_time="t_dat",
        description="Purchases (2), clicks (1) and ignores (0); the app appends to it",
        online_config=BY_CUSTOMER,
    )
    print(
        f"customers={customers.height} articles={articles.height} "
        f"transactions={transactions.height} interactions={interactions.height}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

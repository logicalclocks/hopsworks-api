"""Derive `btc_daily_features` from four shared raw FGs.

Reads `btc_blocks_daily`, `btc_transactions_daily`, `wikipedia_btc_daily`,
`hacker_news_btc_daily` from the shared `hopsworks_default` feature store,
joins on `day`, imputes obvious nulls, engineers a couple of cross-source
features, and writes the result back to the active project as a new FG
named `btc_daily_features`.

Idempotent: re-running upserts the same FG version.
"""

from __future__ import annotations

import hopsworks


def main() -> None:
    project = hopsworks.login()
    print(f"connected to project '{project.name}' (id={project.id})")

    shared_fs = project.get_feature_store(name="hopsworks_default")

    sources = {
        "blocks": shared_fs.get_feature_group("btc_blocks_daily", version=2),
        "txs": shared_fs.get_feature_group("btc_transactions_daily", version=1),
        "wikipedia": shared_fs.get_feature_group("wikipedia_btc_daily", version=1),
        "hacker_news": shared_fs.get_feature_group("hacker_news_btc_daily", version=1),
    }
    for label, fg in sources.items():
        if fg is None:
            raise RuntimeError(f"source FG missing: {label}")
        print(f"resolved {label} -> {fg.name} v{fg.version} (id={fg.id})")

    blocks_df = sources["blocks"].read()
    txs_df = sources["txs"].read()
    wiki_df = sources["wikipedia"].read()
    hn_df = sources["hacker_news"].read()
    print(
        f"raw row counts: blocks={len(blocks_df)} txs={len(txs_df)} "
        f"wiki={len(wiki_df)} hn={len(hn_df)}"
    )

    blocks_df = blocks_df.rename(
        columns={
            "block_count": "blocks_per_day",
            "mean_size": "blocks_mean_size",
            "mean_weight": "blocks_mean_weight",
            "mean_tx_count": "blocks_mean_tx_count",
        }
    )
    txs_df = txs_df.rename(
        columns={
            "tx_count": "txs_per_day",
            "mean_size": "txs_mean_size",
            "mean_input_count": "txs_mean_input_count",
            "mean_output_count": "txs_mean_output_count",
        }
    )
    wiki_df = wiki_df.rename(columns={"total_views": "wiki_views"})
    hn_df = hn_df.rename(
        columns={
            "mention_count": "hn_mentions",
            "mean_score": "hn_mean_score",
            "mean_descendants": "hn_mean_descendants",
        }
    )

    df = (
        blocks_df.merge(txs_df, on="day", how="outer")
        .merge(wiki_df, on="day", how="outer")
        .merge(hn_df, on="day", how="outer")
    )

    # Drop rows where the date itself is null; the rest of the rows we keep
    # and impute, since dropping every row that has any null kills most of
    # the join in a real-world setting.
    df = df.dropna(subset=["day"])
    numeric_cols = [c for c in df.columns if c != "day"]
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # Two cross-source engineered features.
    df["tx_per_block"] = (
        df["txs_per_day"] / df["blocks_per_day"].where(df["blocks_per_day"] > 0, 1)
    )
    df["social_pressure"] = df["wiki_views"] + df["hn_mentions"]

    df = df.sort_values("day")
    print(f"derived row count: {len(df)} cols={list(df.columns)}")

    target_fs = project.get_feature_store()
    derived = target_fs.get_or_create_feature_group(
        name="btc_daily_features",
        version=1,
        primary_key=["day"],
        event_time="day",
        online_enabled=False,
        description=(
            "Daily Bitcoin activity features derived from blocks, "
            "transactions, Wikipedia views, and Hacker News mentions. "
            "Source: hopsworks_default."
        ),
    )
    derived.insert(df, write_options={"start_offline_materialization": True})
    print(f"wrote {len(df)} rows to {derived.name} v{derived.version}")


if __name__ == "__main__":
    main()

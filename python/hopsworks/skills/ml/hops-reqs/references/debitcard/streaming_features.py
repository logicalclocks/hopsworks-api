"""Streaming features: each card's live window aggregates, with Spark Structured Streaming.

    streaming_features.py --window-minutes 60 --watermark-minutes 15

Runs until stopped as a PySpark job; Kafka sources and Structured Streaming do
not run in the Terminal's Spark Connect session. It reads
card_authorisations_live from the project's online Kafka topic, aggregates
each card's authorisations over a tumbling event-time window and upserts the
card's current window into the online feature group card_window_aggregates,
keyed by card_number, where the deployment reads it. A tumbling window gives a
card one open window at a time, so the upsert per card is deterministic.
"""

import argparse
import time

from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

import hopsworks
from hopsworks import build_spark
from hsfs.core import kafka_engine
from hsfs.feature import Feature

SOURCE = "card_authorisations_live"
OUTPUT = "card_window_aggregates"
# A stateful query keeps its shuffle partitions in its checkpoint, so the
# checkpoint directory names the count: a new count needs a new checkpoint.
SHUFFLE_PARTITIONS = 4

AGGREGATES = [
    Feature("card_number", "string", description="16-digit card number"),
    Feature("window_end", "timestamp", description="End of the event-time window"),
    Feature("window_seconds", "int", description="Length of the window"),
    Feature("txn_count", "bigint", description="Authorisations in the window"),
    Feature("approved_count", "bigint"),
    Feature("declined_count", "bigint"),
    Feature("decline_ratio", "double", description="declined_count / txn_count"),
    Feature("queued_count", "bigint", description="Authorisations queued for review"),
    Feature("amount_sum_eur", "double"),
    Feature("amount_avg_eur", "double"),
    Feature("amount_stddev_eur", "double", description="0 with fewer than 2 authorisations"),
    Feature("amount_max_eur", "double"),
    Feature("amount_min_eur", "double"),
    Feature("declined_amount_eur", "double"),
    Feature(
        "cash_withdrawals_sum_eur",
        "double",
        description="Sum of the authorisations' 48-hour cash withdrawals",
    ),
    Feature("distinct_merchants", "bigint", description="Approximate"),
    Feature("distinct_regions", "bigint", description="Approximate"),
    Feature("distinct_entry_modes", "bigint", description="Approximate"),
    Feature("max_merchant_distance_km", "double"),
    Feature(
        "high_risk_count",
        "bigint",
        description="Authorisations with a network or issuer score over 500",
    ),
    Feature("avg_issuer_fraud_score", "double"),
    Feature("max_issuer_fraud_score", "double"),
    Feature("avg_network_risk_score", "double"),
    Feature("max_network_risk_score", "double"),
]


def source_feature_group(fs, timeout_s: int = 180):
    """transaction_stream.py creates the source; on a cold start this job may come up first."""
    deadline = time.time() + timeout_s
    while (fg := fs.get_feature_group(SOURCE, version=1)) is None:
        if time.time() >= deadline:
            raise RuntimeError(
                f"{SOURCE} does not exist after {timeout_s}s: start transaction_stream.py first."
            )
        time.sleep(10)
    return fg


def aggregates(spark, source, window_minutes: int, watermark_minutes: int):
    # Every online feature group of a project shares one Kafka topic; the
    # featureGroupId header says whose a record is, and only the source's
    # records decode with its Avro schema.
    kafka = kafka_engine._get_kafka_config(
        source.feature_store_id, {"internal_kafka": True}, engine="spark"
    )
    raw = (
        spark.readStream.format("kafka")
        .options(**kafka)
        .option("subscribe", source._online_topic_name)
        .option("includeHeaders", "true")
        .load()
        .where(
            F.expr(
                f"exists(headers, h -> h.key = 'featureGroupId' AND string(h.value) = '{source.id}')"
            )
        )
    )
    rows = raw.select(
        from_avro(F.col("value"), source._get_encoded_avro_schema()).alias("r")
    ).select("r.*")

    approved = F.col("authorisation_decision") == "A"
    declined = F.col("authorisation_decision") == "D"
    high_risk = (F.col("issuer_fraud_score") > 500) | (F.col("network_risk_score") > 500)
    agg = (
        rows.withWatermark("transaction_time", f"{watermark_minutes} minutes")
        .groupBy(F.window("transaction_time", f"{window_minutes} minutes"), "card_number")
        .agg(
            F.count(F.lit(1)).alias("txn_count"),
            F.sum(F.when(approved, 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(declined, 1).otherwise(0)).alias("declined_count"),
            F.sum(F.when(F.col("queued_for_review") == "Y", 1).otherwise(0)).alias("queued_count"),
            F.sum("amount_eur").alias("amount_sum_eur"),
            F.avg("amount_eur").alias("amount_avg_eur"),
            F.stddev("amount_eur").alias("amount_stddev_raw"),
            F.max("amount_eur").alias("amount_max_eur"),
            F.min("amount_eur").alias("amount_min_eur"),
            F.sum(F.when(declined, F.col("amount_eur")).otherwise(0.0)).alias(
                "declined_amount_eur"
            ),
            F.sum("cash_withdrawals_48h_eur").alias("cash_withdrawals_sum_eur"),
            F.approx_count_distinct("merchant_id").alias("distinct_merchants"),
            F.approx_count_distinct("authorisation_region").alias("distinct_regions"),
            F.approx_count_distinct("pos_entry_mode").alias("distinct_entry_modes"),
            F.max("merchant_distance_km").alias("max_merchant_distance_km"),
            F.sum(F.when(high_risk, 1).otherwise(0)).alias("high_risk_count"),
            F.avg("issuer_fraud_score").alias("avg_issuer_fraud_score"),
            F.max("issuer_fraud_score").alias("max_issuer_fraud_score"),
            F.avg("network_risk_score").alias("avg_network_risk_score"),
            F.max("network_risk_score").alias("max_network_risk_score"),
        )
    )
    return agg.select(
        "card_number",
        F.col("window.end").alias("window_end"),
        F.lit(window_minutes * 60).cast("int").alias("window_seconds"),
        "txn_count",
        "approved_count",
        "declined_count",
        (F.col("declined_count") / F.col("txn_count")).alias("decline_ratio"),
        "queued_count",
        "amount_sum_eur",
        "amount_avg_eur",
        F.coalesce("amount_stddev_raw", F.lit(0.0)).alias("amount_stddev_eur"),
        "amount_max_eur",
        "amount_min_eur",
        "declined_amount_eur",
        "cash_withdrawals_sum_eur",
        "distinct_merchants",
        "distinct_regions",
        "distinct_entry_modes",
        F.coalesce("max_merchant_distance_km", F.lit(0.0)).alias("max_merchant_distance_km"),
        "high_risk_count",
        "avg_issuer_fraud_score",
        F.col("max_issuer_fraud_score").cast("double").alias("max_issuer_fraud_score"),
        "avg_network_risk_score",
        "max_network_risk_score",
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument(
        "--watermark-minutes", type=int, default=15, help="how late an authorisation may arrive"
    )
    args = parser.parse_args()

    spark = build_spark("card_window_aggregates")
    # About 5 authorisations a second: the default 200 partitions would commit
    # 200 near-empty state stores every micro-batch.
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    project = hopsworks.login()
    fs = project.get_feature_store()
    source = source_feature_group(fs)
    out = aggregates(spark, source, args.window_minutes, args.watermark_minutes)

    fg = fs.get_or_create_feature_group(
        name=OUTPUT,
        version=1,
        description=f"Each card's authorisations over a {args.window_minutes}-minute tumbling window, computed by "
        "Spark Structured Streaming: velocity, amounts, declines, diversity and risk scores. Read online by the "
        "fraud deployment.",
        primary_key=["card_number"],
        event_time="window_end",
        features=AGGREGATES,
        online_enabled=True,
        stream=True,
        statistics_config=False,
        parents=[source],
    )
    checkpoint = f"/Projects/{project.name}/Resources/{OUTPUT}_v1_p{SHUFFLE_PARTITIONS}_checkpoint"
    print(f"Streaming {SOURCE} into {OUTPUT}, checkpoint {checkpoint}")
    fg.insert_stream(
        out,
        query_name=OUTPUT,
        output_mode="update",
        checkpoint_dir=checkpoint,
        await_termination=True,
    )


if __name__ == "__main__":
    main()

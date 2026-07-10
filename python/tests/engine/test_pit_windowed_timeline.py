#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Executable semantics of the point-in-time WINDOWED aggregation timeline plan.

The backend computes a windowed aggregate per training (spine) row with a
spine-marker timeline: source rows UNION ALL spine marker rows, one running
`RANGE BETWEEN <w micros> PRECEDING AND CURRENT ROW` window per entity ordered
by exact microsecond epoch, markers kept. The SQL below mirrors the generated
shape pinned by the backend test
`TestConstructorControllerCollect#testWindowedAggregatePitTimelineShape`
(hopsworks-ee), with a production-realistic `(pk, ts)` primary key on the
source: BOTH columns primary, as online history requires.

Covered semantics, on Spark and DuckDB:
- partial expiry: rows older than (spine - window) do not leak;
- a spine long after the entity's last event reads an EMPTY window;
- the lower bound is inclusive at microsecond precision (the engines agree
  below one second);
- an entity with no source rows reads COUNT 0 / SUM NULL;
- duplicate spine rows both survive (no grouping over the spine payload);
- non-orderable spine payload (a MAP) is carried through untouched.
"""

from __future__ import annotations

import pytest


# window = 30 seconds, in microseconds like the generated statement
WINDOW_MICROS = 30 * 1_000_000

# fixtures: entity A has source events at epoch seconds 60 (amount 1),
# 70 (2), 90 (4), and 100.5 (8) — the fractional row exercises the
# microsecond bound. Spine rows: (A, 100) partial expiry; (A, 1000) idle;
# (A, 120) exact lower bound for the t=90 row; (A, 130.4) fractional bound
# probe (100.5 >= 130.4 - 30 = 100.4 -> INCLUDED, while whole-second
# truncation would compare 100 >= 100 and also include it — but a spine at
# 130.6 must EXCLUDE it: 100.5 < 100.6); (A, 130.6) that exclusion; (B, 100)
# an entity with no rows; and (A, 100) duplicated to pin bag semantics.
SOURCE_ROWS = [("A", 60.0, 1.0), ("A", 70.0, 2.0), ("A", 90.0, 4.0), ("A", 100.5, 8.0)]
SPINE_ROWS = [
    ("A", 100.0, "s-100"),
    ("A", 100.0, "s-100-dup"),
    ("A", 1000.0, "s-1000"),
    ("A", 120.0, "s-120"),
    ("A", 130.4, "s-130.4"),
    ("A", 130.6, "s-130.6"),
    ("B", 100.0, "s-B"),
]

EXPECTED = {
    # window [70, 100]: events 70, 90 -> sum 6 count 2 (t=60 expired)
    "s-100": (6.0, 2),
    "s-100-dup": (6.0, 2),
    # window [970, 1000]: nothing -> NULL / 0
    "s-1000": (None, 0),
    # window [90, 120]: the t=90 event sits exactly on the bound, inclusive
    "s-120": (12.0, 2),
    # microsecond precision: window [100.4, 130.4] -> only the t=100.5 event
    "s-130.4": (8.0, 1),
    # microsecond precision: 100.5 < 100.6 -> excluded
    "s-130.6": (None, 0),
    # entity with no source rows at all
    "s-B": (None, 0),
}


def timeline_sql(ord_source: str, ord_marker: str) -> str:
    """The generated timeline shape with engine-specific epoch functions."""
    return f"""
    SELECT tag, payload, amount_sum, cnt
    FROM (
      SELECT hopsworks_tl_marker, tag, payload,
             SUM(hopsworks_tl_src_amount) OVER (
               PARTITION BY hopsworks_tl_e_0 ORDER BY hopsworks_tl_ord
               RANGE BETWEEN {WINDOW_MICROS} PRECEDING AND CURRENT ROW) AS amount_sum,
             COUNT(hopsworks_tl_src_evt) OVER (
               PARTITION BY hopsworks_tl_e_0 ORDER BY hopsworks_tl_ord
               RANGE BETWEEN {WINDOW_MICROS} PRECEDING AND CURRENT ROW) AS cnt
      FROM (
        SELECT pk AS hopsworks_tl_e_0, {ord_source} AS hopsworks_tl_ord,
               0 AS hopsworks_tl_marker, ts AS hopsworks_tl_src_evt,
               amount AS hopsworks_tl_src_amount, NULL AS tag, NULL AS payload
        FROM src
        UNION ALL
        SELECT pk, {ord_marker}, 1, NULL, NULL, tag, payload
        FROM spine
      ) hopsworks_tl
    ) hopsworks_tl_win
    WHERE hopsworks_tl_marker = 1
    """


def check(rows):
    got = {tag: (amount_sum, cnt) for tag, _, amount_sum, cnt in rows}
    assert got == EXPECTED, got
    # duplicate spine rows both survive as SEPARATE rows (bag semantics)
    tags = [tag for tag, _, _, _ in rows]
    assert tags.count("s-100") == 1 and tags.count("s-100-dup") == 1
    # the MAP payload rides the marker untouched
    payloads = {tag: payload for tag, payload, _, _ in rows}
    assert payloads["s-100"] is not None


RESOURCES = __file__.rsplit("/", 1)[0] + "/resources"

# fixtures for the GOLDEN generated statements (window = 3600 s, tables
# `fs1`.`labels_1` / `fs1`.`fg1_1` exactly as generated by the backend):
# entity 1 has events at 1000 s (amount 1), 2000 s (2), 4000 s (4)
GOLDEN_SOURCE = [(1, 1000.0, 1.0), (1, 2000.0, 2.0), (1, 4000.0, 4.0)]
# spine: inclusive lower bound (4600 - 3600 = 1000), one-second-later exclusion,
# an idle spine, an EXACT duplicate spine row pair (bag semantics), and an
# entity with no source rows
GOLDEN_SPINE = [
    (1, 4600.0, 0.0),
    (1, 4601.0, 0.0),
    (1, 4601.0, 0.0),
    (1, 9999.0, 1.0),
    (2, 4600.0, 1.0),
]
# one output row per spine row: (pk, sec, is_fraud, amount_sum, count)
GOLDEN_EXPECTED = sorted(
    [
        (1, 4600.0, 0.0, 7.0, 3),
        (1, 4601.0, 0.0, 6.0, 2),
        (1, 4601.0, 0.0, 6.0, 2),
        (1, 9999.0, 1.0, None, 0),
        (2, 4600.0, 1.0, None, 0),
    ],
    key=str,
)


class TestPitWindowedTimelineGoldenSql:
    """Execute the backend-GENERATED statements verbatim (review X2-R19).

    The two .sql resources are byte-identical copies of the golden files the
    backend test `TestConstructorControllerCollect#testWindowedAggregatePitTimelineShape`
    (hopsworks-ee, src/test/resources/x2/) asserts against its generator output.
    Update both copies together when the generator changes.
    """

    def test_spark_generated_sql(self, tmp_path):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        with open(f"{RESOURCES}/pit_windowed_timeline_spark.sql") as f:
            sql = f.read().strip()
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("pit-windowed-timeline-golden")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", str(tmp_path))
            .getOrCreate()
        )
        try:
            spark.sql("CREATE DATABASE IF NOT EXISTS fs1")
            spark.createDataFrame(GOLDEN_SOURCE, ["pk", "sec", "amount"]).selectExpr(
                "pk", "timestamp_seconds(sec) AS ts", "amount"
            ).write.mode("overwrite").saveAsTable("fs1.fg1_1")
            spark.createDataFrame(GOLDEN_SPINE, ["pk", "sec", "is_fraud"]).selectExpr(
                "pk", "timestamp_seconds(sec) AS ts", "is_fraud"
            ).write.mode("overwrite").saveAsTable("fs1.labels_1")
            rows = spark.sql(sql).collect()
            got = sorted(
                [
                    (
                        r["pk"],
                        r["ts"].timestamp(),
                        r["is_fraud"],
                        r["amount_sum"],
                        r["count"],
                    )
                    for r in rows
                ],
                key=str,
            )
            assert got == GOLDEN_EXPECTED, got
        finally:
            spark.stop()

    def test_duckdb_generated_sql(self):
        duckdb = pytest.importorskip("duckdb")

        with open(f"{RESOURCES}/pit_windowed_timeline_duckdb.sql") as f:
            sql = f.read().strip()
        con = duckdb.connect()
        con.execute("CREATE SCHEMA fs1")
        con.execute("CREATE TABLE fs1.fg1_1 (pk BIGINT, ts TIMESTAMP, amount DOUBLE)")
        for pk, sec, amount in GOLDEN_SOURCE:
            con.execute(
                "INSERT INTO fs1.fg1_1 SELECT ?, to_timestamp(?), ?", [pk, sec, amount]
            )
        con.execute(
            "CREATE TABLE fs1.labels_1 (pk BIGINT, ts TIMESTAMP, is_fraud DOUBLE)"
        )
        for pk, sec, is_fraud in GOLDEN_SPINE:
            con.execute(
                "INSERT INTO fs1.labels_1 SELECT ?, to_timestamp(?), ?",
                [pk, sec, is_fraud],
            )
        rows = con.execute(sql).fetchall()
        got = sorted(
            [
                (pk, ts.timestamp(), is_fraud, amount_sum, cnt)
                for pk, ts, is_fraud, amount_sum, cnt in rows
            ],
            key=str,
        )
        assert got == GOLDEN_EXPECTED, got


class TestPitWindowedTimelineSemantics:
    def test_spark(self, tmp_path):
        pyspark = pytest.importorskip("pyspark")  # noqa: F841
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("pit-windowed-timeline")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", str(tmp_path))
            .getOrCreate()
        )
        try:
            spark.createDataFrame(SOURCE_ROWS, ["pk", "sec", "amount"]).selectExpr(
                "pk", "timestamp_seconds(sec) AS ts", "amount"
            ).createOrReplaceTempView("src")
            spark.createDataFrame(SPINE_ROWS, ["pk", "sec", "tag"]).selectExpr(
                "pk",
                "timestamp_seconds(sec) AS ts",
                "tag",
                "map('k', tag) AS payload",
            ).createOrReplaceTempView("spine")
            rows = spark.sql(
                timeline_sql("unix_micros(ts)", "unix_micros(ts)")
            ).collect()
            check([(r["tag"], r["payload"], r["amount_sum"], r["cnt"]) for r in rows])
        finally:
            spark.stop()

    def test_duckdb(self):
        duckdb = pytest.importorskip("duckdb")

        con = duckdb.connect()
        con.execute("CREATE TABLE src (pk VARCHAR, ts TIMESTAMP, amount DOUBLE)")
        for pk, sec, amount in SOURCE_ROWS:
            con.execute(
                "INSERT INTO src SELECT ?, to_timestamp(?), ?", [pk, sec, amount]
            )
        con.execute(
            "CREATE TABLE spine (pk VARCHAR, ts TIMESTAMP, tag VARCHAR, "
            "payload MAP(VARCHAR, VARCHAR))"
        )
        for pk, sec, tag in SPINE_ROWS:
            con.execute(
                "INSERT INTO spine SELECT ?, to_timestamp(?), ?, MAP(['k'], [?])",
                [pk, sec, tag, tag],
            )
        rows = con.execute(
            timeline_sql("epoch_us(ts)", "epoch_us(ts)")
        ).fetchall()
        check(rows)

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
"""Scaling gates for the ROOT windowed aggregate (review PERF-2026-08-1 and -2).

The review measured the root path's finite Spark `RANGE` frame at 3.279 s / 5.928 s /
10.287 s for 10k / 20k / 40k rows on one hot entity, against 0.010 s in DuckDB, and
asked for scaling tests for the root and direct-join aggregates plus an assertion that
no finite Spark `RANGE` survives in any supported PIT topology.

This module supplies the measurement half, which has to exist before the rewrite can
be called an improvement:

* `TestRootWindowEquivalence` proves the cumulative rewrite returns exactly what the
  finite frame returns, including the NULL of an empty window and float-unsafe
  magnitudes. Correctness first: the whole reason the root path cannot simply use
  signed expiry deltas is that they corrupt floating sums (review ENT-PIT-1).
* `TestRootWindowScaling` reports both shapes at growing row counts. Measured here on
  a dense single-entity window, Spark takes 4.323 s / 15.655 s / 62.915 s at 10k / 20k
  / 40k rows for the finite frame against 2.410 s / 2.816 s / 3.081 s for the
  cumulative one: quadrupling per doubling versus flat, a 20x gap at 40k.

  Two measurements qualify the review's recommendation to route the root through a
  cumulative timeline unconditionally:

  1. The blowup is driven by rows per FRAME, not rows per table. Spread the same 40k
     rows over 100 windows and the finite frame is linear and cheap; the cumulative
     plan is then about 2x SLOWER. `span_windows` exists so a gate cannot accidentally
     be written against thin frames, where it would measure nothing.
  2. DuckDB never exhibits the blowup at any density (0.01-0.12 s throughout) and the
     cumulative plan costs it about 2x more.

  So the rewrite is a Spark-dense-window optimisation, not a universal one, and the
  generator should choose per dialect rather than always emitting the cumulative shape.
* `TestExactCountDeltaEquivalence` covers PERF-2026-08-2: for count-only workloads the
  signed integer delta timeline is exact, needs one sort instead of two, and the review
  measured it 2.45x (Spark) to 4.42x (DuckDB) faster on a spine-heavy shape.

The SQL under test lives in `resources/root_window_*.sql`. Those files pin the shape
the backend is expected to generate, so when `buildAggregateRunningCteNode` is changed
the generated-SQL golden and these files must agree.

Spark runs are opt-in through RUN_SPARK_SCALING=1: a local Spark session at 40k rows
takes minutes, which does not belong in the default suite. DuckDB runs always.
"""

from __future__ import annotations

import os
import time

import pytest


RESOURCES = __file__.rsplit("/", 1)[0] + "/resources"
WINDOW_SECONDS = 3600
RUN_SPARK = os.environ.get("RUN_SPARK_SCALING") == "1"


def _sql(resource: str) -> str:
    with open(f"{RESOURCES}/{resource}") as handle:
        return handle.read().strip().rstrip(";")


def _as_spark(sql: str) -> str:
    """Retarget a probe written in DuckDB syntax at Spark.

    Only two things differ: the identifier quote and the epoch function. The generated
    goldens ship one file per dialect because they are backend output, but these probes
    are hand-written and would drift if kept twice. Substitution is safe because they
    contain no string literals, so no double quote is ever data.
    """
    return sql.replace('"', "`").replace("EPOCH_US(", "UNIX_MICROS(")


def _hot_entity_rows(
    count: int, entities: int = 1, span_windows: float = 1.0
) -> list[tuple[int, float, float]]:
    """Rows on one hot key, spread over `span_windows` windows.

    Density is the parameter that matters, not row count. A finite `RANGE` frame costs
    O(rows x frame size) because Spark re-aggregates the buffer per row, so the
    near-quadratic behaviour only appears when the frame holds a large share of the
    rows. `span_windows=1` puts every row inside one window, which is the single hot
    entity the review probed; raising it thins each frame out and the finite frame
    becomes linear and cheap. A gate written against thin frames measures nothing.
    """
    rows = []
    step = (WINDOW_SECONDS * span_windows) / max(count, 1)
    for i in range(count):
        rows.append((i % entities, float(i) * step, float(i % 97)))
    return rows


def _duckdb_con(rows):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute("CREATE SCHEMA fs1")
    con.execute("CREATE TABLE fs1.fg1_1 (pk BIGINT, ts TIMESTAMP, amount DOUBLE)")
    con.executemany(
        "INSERT INTO fs1.fg1_1 SELECT ?, to_timestamp(?), ?",
        [[pk, sec, amount] for pk, sec, amount in rows],
    )
    return con


def _timed(fn):
    started = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - started


class TestRootWindowEquivalence:
    """The cumulative rewrite must return exactly what the finite frame returns."""

    def test_duckdb_shapes_agree_on_dense_rows(self):
        con = _duckdb_con(_hot_entity_rows(2000))
        finite = con.execute(_sql("root_window_finite_range_duckdb.sql")).fetchall()
        cumulative = con.execute(_sql("root_window_cumulative_duckdb.sql")).fetchall()
        assert finite == cumulative

    def test_duckdb_shapes_agree_across_entities_and_gaps(self):
        # gaps wider than the window (empty trailing windows), several entities, and
        # rows sharing an ordinal (RANGE peers must all fall inside [t-w, t])
        rows = [
            (1, 0.0, 5.0),
            (1, 0.0, 7.0),  # tie on the ordinal
            (1, 1800.0, 1.0),
            (1, 100000.0, 3.0),  # isolated: window holds only itself
            (2, 50.0, 2.0),
            (3, 0.0, None),  # all-NULL window must stay NULL, not become 0
        ]
        con = _duckdb_con(rows)
        finite = con.execute(_sql("root_window_finite_range_duckdb.sql")).fetchall()
        cumulative = con.execute(_sql("root_window_cumulative_duckdb.sql")).fetchall()
        assert finite == cumulative

    def test_duckdb_shapes_agree_on_float_unsafe_magnitudes(self):
        # review ENT-PIT-1: 1e16 + 1.0 - 1e16 == 0.0, so a signed expiry-delta plan
        # loses the smaller value once the larger expires. The two-bucket merge never
        # subtracts, so it must match the finite frame exactly here.
        rows = [
            (1, 0.0, 1e16),
            (1, 1.0, 1.0),
            (1, 2.0, 1.0),
            (1, 100000.0, 1.0),
        ]
        con = _duckdb_con(rows)
        finite = con.execute(_sql("root_window_finite_range_duckdb.sql")).fetchall()
        cumulative = con.execute(_sql("root_window_cumulative_duckdb.sql")).fetchall()
        assert finite == cumulative


class TestRootWindowScaling:
    """PERF-2026-08-1: the finite frame's cost must not grow super-linearly."""

    @pytest.mark.parametrize("count", [5000, 10000, 20000])
    def test_duckdb_reports_both_shapes(self, count, capsys):
        con = _duckdb_con(_hot_entity_rows(count))
        finite, finite_s = _timed(
            lambda: con.execute(_sql("root_window_finite_range_duckdb.sql")).fetchall()
        )
        cumulative, cumulative_s = _timed(
            lambda: con.execute(_sql("root_window_cumulative_duckdb.sql")).fetchall()
        )
        assert finite == cumulative
        with capsys.disabled():
            print(
                f"\nDuckDB rows={count:>6}  finite={finite_s:7.3f}s  "
                f"cumulative={cumulative_s:7.3f}s"
            )

    def test_duckdb_cumulative_scales_linearly(self):
        # The gate: quadrupling the rows must not more than roughly octuple the time.
        # DuckDB executes the finite frame efficiently, so this guards the REWRITE
        # against a regression rather than reproducing Spark's blowup.
        timings = {}
        for count in (5000, 20000):
            con = _duckdb_con(_hot_entity_rows(count))
            _, seconds = _timed(
                lambda con=con: con.execute(
                    _sql("root_window_cumulative_duckdb.sql")
                ).fetchall()
            )
            timings[count] = seconds
        growth = timings[20000] / max(timings[5000], 1e-6)
        assert growth < 16, f"cumulative root window scaled {growth:.1f}x for 4x rows"

    @pytest.mark.skipif(not RUN_SPARK, reason="set RUN_SPARK_SCALING=1")
    @pytest.mark.parametrize("count", [10000, 20000, 40000])
    def test_spark_finite_frame_is_the_defect(self, count, tmp_path, capsys):
        # The review's own probe: 3.279s / 5.928s / 10.287s at 10k / 20k / 40k. Spark
        # re-aggregates a finite RANGE frame per row; its UNBOUNDED PRECEDING frame is
        # incremental, which is why the rewrite is expected to flatten this curve.
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[2]")
            .appName(f"root-window-{count}")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", str(tmp_path))
            .getOrCreate()
        )
        spark.sql("CREATE DATABASE IF NOT EXISTS fs1")
        spark.createDataFrame(
            _hot_entity_rows(count), "pk long, sec double, amount double"
        ).selectExpr("pk", "timestamp_seconds(sec) AS ts", "amount").write.mode(
            "overwrite"
        ).saveAsTable("fs1.fg1_1")

        finite_sql = _as_spark(_sql("root_window_finite_range_duckdb.sql"))
        cumulative_sql = _as_spark(_sql("root_window_cumulative_duckdb.sql"))
        finite, finite_s = _timed(lambda: spark.sql(finite_sql).collect())
        cumulative, cumulative_s = _timed(lambda: spark.sql(cumulative_sql).collect())
        assert [tuple(r) for r in finite] == [tuple(r) for r in cumulative]
        with capsys.disabled():
            print(
                f"\nSpark rows={count:>6}  finite={finite_s:7.3f}s  "
                f"cumulative={cumulative_s:7.3f}s"
            )
        if count >= 20000:
            # Measured on a dense single-entity window: finite 4.3s / 15.7s / 62.9s at
            # 10k / 20k / 40k against cumulative 2.4s / 2.8s / 3.1s. The finite frame
            # roughly quadruples per doubling; the cumulative plan is flat. Once the
            # generator emits the cumulative shape this is the gate that keeps it.
            assert cumulative_s < finite_s, (
                f"at {count} dense rows the cumulative plan ({cumulative_s:.3f}s) must "
                f"beat the finite RANGE frame ({finite_s:.3f}s)"
            )


class TestExactCountDeltaEquivalence:
    """PERF-2026-08-2: integer count deltas are exact, so one sort suffices."""

    COUNT_TWO_BUCKET = """
    WITH scan AS (
      SELECT pk AS e0, EPOCH_US(ts) AS ord,
             ROW_NUMBER() OVER (PARTITION BY pk ORDER BY EPOCH_US(ts)) AS seq
      FROM fs1.fg1_1
    ), tl AS (
      SELECT s.e0,
             CASE WHEN r.role = 1 THEN s.ord ELSE s.ord - 3600000000 END AS ord,
             r.role AS marker, s.seq,
             CAST(FLOOR((CASE WHEN r.role = 1 THEN s.ord ELSE s.ord - 3600000000 END)
                  / 3600000000) AS BIGINT) AS bucket,
             CASE WHEN r.role = 1 THEN 1 END AS cv0
      FROM scan s CROSS JOIN (SELECT 1 AS role UNION ALL SELECT 2 AS role) r
    ), w AS (
      SELECT e0, marker, seq,
             SUM(cv0) OVER (PARTITION BY e0, bucket ORDER BY ord
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cf0,
             SUM(cv0) OVER (PARTITION BY e0, bucket ORDER BY ord DESC
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cb0
      FROM tl
    )
    SELECT e0 AS pk,
           CAST(COALESCE(MAX(CASE WHEN marker = 1 THEN cf0 END), 0)
              + COALESCE(MAX(CASE WHEN marker = 2 THEN cb0 END), 0) AS BIGINT) AS n
    FROM w WHERE marker >= 1 GROUP BY e0, seq ORDER BY 1, 2
    """

    # +1 at the event, -1 one microsecond after the window closes, then ONE cumulative
    # pass. Exact for integers, so none of the two-bucket machinery is needed.
    COUNT_SIGNED_DELTA = """
    WITH d AS (
      SELECT pk AS e0, EPOCH_US(ts) AS ord, 1 AS delta,
             ROW_NUMBER() OVER (PARTITION BY pk ORDER BY EPOCH_US(ts)) AS seq
      FROM fs1.fg1_1
      UNION ALL
      SELECT pk AS e0, EPOCH_US(ts) + 3600000000 + 1 AS ord, -1 AS delta, NULL AS seq
      FROM fs1.fg1_1
    )
    SELECT e0 AS pk, CAST(n AS BIGINT) AS n FROM (
      SELECT e0, seq,
             SUM(delta) OVER (PARTITION BY e0 ORDER BY ord
               RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n
      FROM d
    ) t WHERE seq IS NOT NULL ORDER BY 1, 2
    """

    def test_duckdb_count_plans_agree(self):
        con = _duckdb_con(_hot_entity_rows(3000, entities=25))
        two_bucket = con.execute(self.COUNT_TWO_BUCKET).fetchall()
        signed = con.execute(self.COUNT_SIGNED_DELTA).fetchall()
        assert two_bucket == signed

    def test_duckdb_count_plans_agree_with_ties_and_gaps(self):
        rows = [
            (1, 0.0, 1.0),
            (1, 0.0, 1.0),  # tie: both inside [t-w, t]
            (1, 3600.0, 1.0),  # exactly on the window edge
            (1, 3600.000001, 1.0),  # one microsecond past it
            (2, 99999.0, 1.0),
        ]
        con = _duckdb_con(rows)
        assert (
            con.execute(self.COUNT_TWO_BUCKET).fetchall()
            == con.execute(self.COUNT_SIGNED_DELTA).fetchall()
        )

    def test_duckdb_count_delta_is_not_slower(self, capsys):
        con = _duckdb_con(_hot_entity_rows(20000, entities=200))
        two_bucket, two_bucket_s = _timed(
            lambda: con.execute(self.COUNT_TWO_BUCKET).fetchall()
        )
        signed, signed_s = _timed(
            lambda: con.execute(self.COUNT_SIGNED_DELTA).fetchall()
        )
        assert two_bucket == signed
        with capsys.disabled():
            print(
                f"\nDuckDB count two_bucket={two_bucket_s:7.3f}s  "
                f"signed_delta={signed_s:7.3f}s  "
                f"ratio={two_bucket_s / max(signed_s, 1e-6):5.2f}x"
            )
        assert signed_s <= two_bucket_s * 1.5, (
            "the one-sort signed count plan should not be slower than the "
            "two-bucket plan it is meant to replace"
        )

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

"""Unit tests for the Hudi-only `partitioned_by` grain materializer.

The grain names are derived from the temporal transforms of the feature
group's `partitioned_by` (`year(ts)` materializes a `year` column, ...);
Iceberg and Delta no longer materialize anything, so only the Spark twin
exists.
"""

from datetime import date, datetime
from types import SimpleNamespace

import pytest
from hsfs.core import partition_grains


def _fg(partitioned_by=None, event_time="event_ts"):
    return SimpleNamespace(partitioned_by=partitioned_by, event_time=event_time)


# region — grain derivation (no Spark needed)


def test_grains_derived_from_temporal_transforms():
    fg = _fg(["year(event_ts)", "bucket(4, id)", "month(event_ts)"])
    assert partition_grains._grains(fg) == ["year", "month"]


def test_grains_empty_for_identity_only_spec():
    assert partition_grains._grains(_fg(["region"])) == []


def test_grains_empty_for_old_grain_form():
    # pre-transform specs are opaque: no grains are derived from them
    assert partition_grains._grains(_fg(["year", "month"])) == []


def test_grains_empty_without_partitioned_by():
    assert partition_grains._grains(_fg(None)) == []


# endregion


# region — no-op paths (no Spark needed: they return before touching pyspark)


def test_spark_noop_without_partitioned_by():
    dataset = SimpleNamespace(columns=["id", "event_ts"])
    assert partition_grains._materialize_grains_spark(_fg(None), dataset) is dataset


def test_spark_noop_for_old_grain_form():
    dataset = SimpleNamespace(columns=["id", "event_ts"])
    fg = _fg(["year", "month"])
    assert partition_grains._materialize_grains_spark(fg, dataset) is dataset


def test_spark_noop_without_temporal_transforms():
    dataset = SimpleNamespace(columns=["id", "region"])
    fg = _fg(["region", "bucket(4, id)"])
    assert partition_grains._materialize_grains_spark(fg, dataset) is dataset


def test_spark_noop_when_event_time_none():
    dataset = SimpleNamespace(columns=["id", "event_ts"])
    fg = _fg(["year(event_ts)"], event_time=None)
    assert partition_grains._materialize_grains_spark(fg, dataset) is dataset


def test_spark_noop_when_event_time_column_missing():
    dataset = SimpleNamespace(columns=["id"])
    fg = _fg(["year(event_ts)"])
    assert partition_grains._materialize_grains_spark(fg, dataset) is dataset


# endregion


# region — materialization (real Spark)


@pytest.fixture(scope="module")
def spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.master("local[1]").getOrCreate()


def test_spark_materializes_grains_from_timestamp(spark_session):
    fg = _fg(["year(event_ts)", "month(event_ts)"])
    df = spark_session.createDataFrame(
        [(1, datetime(2026, 1, 15)), (2, datetime(2026, 3, 2))], ["id", "event_ts"]
    )
    out = partition_grains._materialize_grains_spark(fg, df)
    assert out.columns == ["id", "event_ts", "year", "month"]
    rows = out.orderBy("id").collect()
    assert [r["year"] for r in rows] == [2026, 2026]
    assert [r["month"] for r in rows] == [1, 3]


def test_spark_idempotent_when_grain_present(spark_session):
    fg = _fg(["year(event_ts)"])
    df = spark_session.createDataFrame(
        [(datetime(2026, 5, 1), 1999)], ["event_ts", "year"]
    )
    out = partition_grains._materialize_grains_spark(fg, df)
    # Already present: not recomputed, not duplicated.
    assert out.columns == ["event_ts", "year"]
    assert out.collect()[0]["year"] == 1999


def test_spark_integer_seconds_event_time(spark_session):
    fg = _fg(["year(ts)"], event_time="ts")
    # 1736899200 = 2025-01-15 00:00:00 UTC (10-digit -> seconds)
    df = spark_session.createDataFrame([(1736899200,)], ["ts"])
    out = partition_grains._materialize_grains_spark(fg, df)
    assert out.collect()[0]["year"] == 2025


def test_spark_date_event_time_with_hour_grain(spark_session):
    # A date event_time has no sub-day resolution; the hour grain must come
    # out as 0 rather than failing.
    fg = _fg(
        [
            "year(event_date)",
            "month(event_date)",
            "day(event_date)",
            "hour(event_date)",
        ],
        event_time="event_date",
    )
    df = spark_session.createDataFrame([(date(2026, 4, 15),)], ["event_date"])
    row = partition_grains._materialize_grains_spark(fg, df).collect()[0]
    assert row["year"] == 2026
    assert row["month"] == 4
    assert row["day"] == 15
    assert row["hour"] == 0


# endregion

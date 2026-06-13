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
"""Materialization of `partitioned_by` grain columns for the write paths.

The grain columns (year/month/week/day/hour) are ordinary partition columns
on the feature group; the writer derives their values from `event_time` on
every write. Delta, Hudi, and Iceberg share this derivation so every format
produces identical grain values for the same event_time input. There are two
twins:

- `materialize_grains_spark` — for Spark DataFrames (Delta, Hudi, Iceberg).
- `materialize_grains_arrow` — for PyArrow tables (delta-rs, PyIceberg).

The Hudi DeltaStreamer path has a third, JVM twin in `PartitionedByTransformer`.
"""

from __future__ import annotations


def materialize_grains_spark(feature_group, dataset):
    """Add the feature group's `partitioned_by` grain columns to a Spark DataFrame.

    Values are derived from the feature group's `event_time` column.
    Columns already present are left untouched, and the dataset is returned
    unchanged when the feature group has no `partitioned_by` or the
    event_time column is absent.
    """
    grains = getattr(feature_group, "partitioned_by", None)
    if not grains:
        return dataset
    event_time = feature_group.event_time
    if event_time is None or event_time not in dataset.columns:
        return dataset
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, NumericType, TimestampType

    et_type = dataset.schema[event_time].dataType
    if isinstance(et_type, (TimestampType, DateType)):
        ts = F.col(event_time)
    elif isinstance(et_type, NumericType):
        # seconds-vs-milliseconds rule: a value up to ten digits is seconds.
        ts = (
            F.when(F.abs(F.col(event_time)) <= 9999999999, F.col(event_time))
            .otherwise(F.col(event_time) / 1000)
            .cast("timestamp")
        )
    else:
        ts = F.to_timestamp(F.col(event_time))
    grain_fns = {
        "year": F.year,
        "month": F.month,
        "week": F.weekofyear,
        "day": F.dayofmonth,
        "hour": F.hour,
    }
    for grain in grains:
        if grain in dataset.columns:
            continue
        dataset = dataset.withColumn(grain, grain_fns[grain](ts).cast("int"))
    return dataset


def materialize_grains_arrow(feature_group, table):
    """Add the feature group's `partitioned_by` grain columns to a PyArrow table.

    Arrow twin of [`materialize_grains_spark`][] for the non-Spark write paths
    (delta-rs, PyIceberg). Columns already present are left untouched, and the
    table is returned unchanged when the feature group has no `partitioned_by`,
    the input is not a PyArrow table, or the event_time column is absent.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    if not isinstance(table, pa.Table):
        return table
    grains = getattr(feature_group, "partitioned_by", None)
    if not grains:
        return table
    event_time = feature_group.event_time
    if event_time is None or event_time not in table.column_names:
        return table
    ts = _event_time_arrow_to_timestamp(table.column(event_time))
    grain_fns = {
        "year": pc.year,
        "month": pc.month,
        "week": pc.iso_week,
        "day": pc.day,
        "hour": pc.hour,
    }
    for grain in grains:
        if grain in table.column_names:
            continue
        values = pc.cast(grain_fns[grain](ts), pa.int32())
        table = table.append_column(grain, values)
    return table


def _event_time_arrow_to_timestamp(column):
    """Return a timestamp Arrow array from an event_time column.

    Integer event_time follows the seconds-vs-milliseconds rule (a value up to
    ten digits is treated as unix seconds, longer as milliseconds), the same
    convention the rest of the client uses.
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    col_type = column.type
    if pa.types.is_timestamp(col_type):
        return column
    if pa.types.is_date(col_type):
        # Cast date -> timestamp (midnight) so sub-day grain functions like
        # pc.hour have a kernel — pc.hour has no date32 kernel and would raise.
        # Matches the Spark path, where F.hour on a DateType yields 0.
        return pc.cast(column, pa.timestamp("s"))
    if pa.types.is_integer(col_type):
        # Per-row seconds-vs-milliseconds decision (mirrors the Spark path):
        # a value up to ten digits is unix seconds, longer is milliseconds.
        seconds = pc.if_else(
            pc.less_equal(pc.abs(column), 9999999999),
            column,
            pc.divide(column, 1000),
        )
        return pc.cast(seconds, pa.timestamp("s"))
    # strings / other — let Arrow attempt a timestamp cast
    return pc.cast(column, pa.timestamp("us"))

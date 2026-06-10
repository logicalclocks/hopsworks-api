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
"""Materialization of `partitioned_by` grain columns for Spark writers.

The grain columns (year/month/week/day/hour) are ordinary partition columns
on the feature group; the writer derives their values from `event_time` on
every write. The Delta and Hudi Spark write paths share this derivation so
both formats produce identical grain values for the same event_time input
(the delta-rs path has an Arrow twin in `DeltaEngine`, and the Hudi
DeltaStreamer path a JVM twin in `PartitionedByTransformer`).
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

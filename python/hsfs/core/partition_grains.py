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
"""Materialization of Hudi `partitioned_by` grain columns for the write path.

Hudi has no hidden partitioning, so the temporal transforms of
`partitioned_by` (`year(event_time)`, `month(event_time)`, ...) become
ordinary INT partition columns named after the grain; the writer derives
their values from `event_time` on every write. Iceberg and Delta do not use
this module: Iceberg compiles the transforms into its partition spec and
Delta liquid-clusters on the source columns.
"""

from __future__ import annotations

from hsfs.core import partition_transforms


def _grains(feature_group) -> list[str]:
    """Return the temporal grain column names of the feature group's spec."""
    transforms = partition_transforms._try_parse(
        getattr(feature_group, "partitioned_by", None)
    )
    return partition_transforms._temporal_grains(transforms or [])


def _materialize_grains_spark(feature_group, dataset):
    """Add the feature group's grain columns to a Spark DataFrame.

    Values are derived from the feature group's `event_time` column.
    Columns already present are left untouched, and the dataset is returned
    unchanged when the feature group has no temporal transforms or the
    event_time column is absent.
    """
    grains = _grains(feature_group)
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

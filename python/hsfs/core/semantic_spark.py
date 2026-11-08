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
"""Runs a semantic query's Spark SQL on the caller's Spark session, over raw views of the feature groups it reads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import engine
from hsfs.core import feature_group_api, feature_store_api


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


_SPARK_ENGINES = ("spark", "spark-no-metastore", "spark-delta")
_UTC = ("UTC", "Etc/UTC")
# The attribute of a returned DataFrame that lists the views its call registered.
_VIEWS = "_hopsworks_semantic_views"


@dataclass
class _RawAlias:
    """A feature group read whole at its latest version, in the shape the engine's table registration takes."""

    feature_group: Any
    alias: str
    left_feature_group_start_timestamp: int | None = None
    left_feature_group_end_timestamp: int | None = None


def _session(spark: SparkSession | None) -> SparkSession:
    """`spark`, or the active session; checked against hsfs's engine without letting the engine set the time zone first.

    The engine sets `spark.sql.session.timeZone` to UTC when it starts, which would hide a session the caller configured otherwise, so the time zone is read before the engine is first asked for.
    """
    if engine._get_type() not in _SPARK_ENGINES:
        raise FeatureStoreException(
            "engine='spark' runs the query on Spark: connect to Hopsworks with a Spark engine, from a Spark job or notebook."
        )
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
    if spark is None and engine._engine is not None:
        spark = engine._engine._spark_session
    if spark is None:
        raise FeatureStoreException(
            "There is no active Spark session; pass one as spark=."
        )
    time_zone = spark.conf.get("spark.sql.session.timeZone")
    if time_zone not in _UTC:
        raise FeatureStoreException(
            f"spark.sql.session.timeZone is {time_zone!r}; semantic queries on Spark need UTC, which this session must already use."
        )
    if engine._get_instance()._spark_session is not spark:
        raise FeatureStoreException(
            "The Spark session is not the one the hsfs engine holds, which registers the views; pass that session, or none."
        )
    return spark


def _register(views: list[dict[str, Any]], registered: list[str]) -> None:
    """Registers each view over its feature group's latest data, unfiltered, adding its alias to `registered`.

    Never through `fg.read()`, which applies the scheduler's `HOPS_START_TIME` and `HOPS_END_TIME`.
    """
    spark_engine = engine._get_instance()
    stores = {}
    groups = feature_group_api.FeatureGroupApi()
    for view in views:
        store_name = view["featureStore"]
        if store_name not in stores:
            stores[store_name] = feature_store_api.FeatureStoreApi()._get(store_name)
        store = stores[store_name]
        fg = groups._get_by_id(store.id, view["featureGroupId"])
        fmt = (getattr(fg, "time_travel_format", None) or "NONE").upper()
        raw = _RawAlias(fg, view["alias"])
        if fmt == "DELTA":
            spark_engine._register_delta_temporary_table(
                delta_fg_alias=raw,
                feature_store_id=store.id,
                feature_store_name=store.name,
                read_options=None,
            )
        elif fmt == "HUDI" and not spark_engine._is_connect:
            spark_engine._register_hudi_temporary_table(raw, store.id, store.name, None)
        else:
            raise FeatureStoreException(
                f"Feature group {fg.name} version {fg.version} is {fmt}"
                + (" on Spark Connect" if fmt == "HUDI" else "")
                + "; semantic queries on Spark read Delta feature groups, and Hudi ones on classic Spark."
            )
        registered.append(view["alias"])


def _query(answer: dict[str, Any], spark: SparkSession | None) -> DataFrame:
    """Runs the gateway's Spark `answer` (`sql` and `views`) and returns its DataFrame, which lists the views for `_drop_views`."""
    session = _session(spark)
    registered: list[str] = []
    try:
        _register(answer["views"], registered)
        df = session.sql(answer["sql"])
    except Exception:
        for alias in registered:
            session.catalog.dropTempView(alias)
        raise
    setattr(df, _VIEWS, registered)
    return df


def _drop_views(df: DataFrame) -> None:
    """Drops the temporary views the call that returned `df` registered; `df` cannot be read after."""
    views = getattr(df, _VIEWS, None)
    if views is None:
        raise ValueError(
            "The DataFrame did not come from a semantic query with engine='spark'."
        )
    for alias in views:
        df.sparkSession.catalog.dropTempView(alias)

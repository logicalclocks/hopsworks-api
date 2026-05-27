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
"""Utilities for detecting Spark Connect (thin gRPC client) sessions."""

from __future__ import annotations

import os


def is_spark_connect_env() -> bool:
    """Detect Spark Connect mode before a SparkSession is available.

    Checks the ``SPARK_CONNECT_MODE_ENABLED`` environment variable that PySpark
    sets when a Spark Connect session is created, and falls back to
    ``pyspark.sql.utils.is_remote()`` when available.

    Returns:
        `True` if running in Spark Connect mode, `False` otherwise.
    """
    if os.environ.get("SPARK_CONNECT_MODE_ENABLED", "").lower() in ("1", "true"):
        return True
    try:
        from pyspark.sql.utils import is_remote

        return is_remote()
    except ImportError:
        return False


def is_spark_connect_session(spark_session) -> bool:
    """Detect whether *spark_session* is a Spark Connect session.

    Uses :func:`is_spark_connect_env` first. If that's inconclusive, prefer a
    direct module-class check (Connect's ``SparkSession`` lives in
    ``pyspark.sql.connect.session``) before falling back to probing
    ``spark_session.sparkContext`` — which raises various PySpark exception
    classes in Connect mode (``PySparkNotImplementedError``,
    ``PySparkAttributeError``, …) that do not all subclass the stdlib
    ``NotImplementedError``/``AttributeError``.

    Parameters:
        spark_session: The SparkSession instance to check.

    Returns:
        `True` if the session is a Spark Connect session, `False` otherwise.
    """
    if is_spark_connect_env():
        return True
    # Direct check: Connect's SparkSession lives in a different module.
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession

        if isinstance(spark_session, ConnectSession):
            return True
    except ImportError:
        pass
    # Fallback probe: classic SparkSession exposes ``sparkContext``; Connect
    # raises a PySpark-specific exception. Catch broadly so a future PySpark
    # exception class does not leak through and make us claim "classic".
    try:
        _ = spark_session.sparkContext
        return False
    except Exception:  # noqa: BLE001 - intentionally broad; Connect raises a moving target
        return True


def is_spark_dataframe(obj) -> bool:
    """Return True for both classic and Spark Connect DataFrames.

    The classic ``pyspark.sql.DataFrame`` and the Spark Connect
    ``pyspark.sql.connect.dataframe.DataFrame`` are API-compatible but
    distinct classes, so a single ``isinstance`` against the classic class
    rejects Connect DataFrames. This helper checks both.

    Both imports are wrapped in ``try``/``except ImportError`` to handle
    older PySparks that don't ship the ``connect`` submodule and environments
    where pyspark is not installed at all.

    Parameters:
        obj: Any object to type-test.

    Returns:
        `True` if *obj* is a Spark DataFrame in either mode, `False` otherwise.
    """
    try:
        from pyspark.sql import DataFrame as ClassicDataFrame

        if isinstance(obj, ClassicDataFrame):
            return True
    except ImportError:
        pass
    try:
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        if isinstance(obj, ConnectDataFrame):
            return True
    except ImportError:
        pass
    return False

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

    Uses :func:`is_spark_connect_env` first.
    As a last resort, attempts to access ``spark_session.sparkContext`` which
    raises ``PySparkNotImplementedError`` in Connect mode.

    Parameters:
        spark_session: The SparkSession instance to check.

    Returns:
        `True` if the session is a Spark Connect session, `False` otherwise.
    """
    if is_spark_connect_env():
        return True
    try:
        _ = spark_session.sparkContext
        return False
    except (NotImplementedError, AttributeError):
        return True

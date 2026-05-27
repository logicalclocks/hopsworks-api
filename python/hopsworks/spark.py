"""SparkSession builder for Hopsworks.

In Spark Connect mode (the terminal-spark image's bashrc sets
``SPARK_REMOTE``, and PySpark also flips ``SPARK_CONNECT_MODE_ENABLED``
once a Connect session is created), Hopsworks reads and writes offline
feature groups through Delta Lake and the bare ``SparkSession.builder``
does not load the Delta extensions or rewire the default catalog to
``DeltaCatalog`` — every ``fg.read()`` / ``fg.insert(df)`` then silently
misbehaves. ``build_spark`` returns a Connect session with both configs
pre-applied so this class of failure becomes impossible by construction.

For non-Connect runs (spark-submit, classic spark-on-yarn, an external
cluster) the cluster's spark-defaults.conf is the source of truth and we
do **not** override its session config from here. The Delta defaults are
applied only in Spark Connect mode.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from hopsworks_apigen import public
from hopsworks_common.spark_connect_utils import is_spark_connect_env


if TYPE_CHECKING:
    from pyspark.sql import SparkSession


_DELTA_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


@public
def build_spark(
    app_name: str = "hopsworks",
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Return a ``SparkSession`` configured for the Hopsworks runtime.

    In Spark Connect mode the returned session has Delta Lake's SQL
    extension and DeltaCatalog wired in, so every Hopsworks
    offline-feature-group read or write works without further user
    setup. ``extra_configs`` is layered on top, so callers can still
    add their own configs (e.g. ``spark.sql.shuffle.partitions``).

    Outside Spark Connect, the helper does not inject the Delta defaults
    — classic Spark deployments expect their cluster's
    ``spark-defaults.conf`` to be authoritative, and overriding it from
    here can mask real misconfigurations. Only ``app_name`` and
    ``extra_configs`` flow through.

    Spark Connect detection delegates to
    ``hopsworks_common.spark_connect_utils.is_spark_connect_env`` (which
    checks ``SPARK_CONNECT_MODE_ENABLED`` and falls back to
    ``pyspark.sql.utils.is_remote()``); the latter also covers the
    ``SPARK_REMOTE`` env var that the terminal-spark image sets. The
    helper never calls ``.remote(...)`` itself — hard-coding the URI
    breaks the moment the script runs anywhere other than that one
    terminal pod.

    Parameters:
        app_name: Spark application name; surfaces in the Spark UI.
        extra_configs: Additional ``spark.<key> = <value>`` configs.

    Returns:
        A configured ``SparkSession``.

    Raises:
        ImportError: If PySpark is not installed. The terminal-spark
            image ships PySpark; outside it, install ``pyspark[connect]``.

    Example:
        ```python
        from hopsworks import build_spark

        spark = build_spark("my_pipeline")
        df = spark.read.format("delta").load("/hopsfs/.../my_table")
        ```
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(
            "build_spark() requires pyspark. Run inside the Hopsworks "
            "terminal-spark image or install pyspark[connect] in your venv."
        ) from exc

    builder = SparkSession.builder.appName(app_name)
    if is_spark_connect_env():
        builder = builder.config("spark.sql.extensions", _DELTA_EXTENSIONS)
        builder = builder.config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()

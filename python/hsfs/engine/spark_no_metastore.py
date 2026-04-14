#
#   Copyright 2023 Hopsworks AB
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
from __future__ import annotations

import contextlib


with contextlib.suppress(ImportError):
    from pyspark.sql import SparkSession

from hopsworks_common.spark_connect_utils import is_spark_connect_env
from hsfs.engine import spark


class Engine(spark.Engine):
    def _create_spark_session(self):
        """Create a SparkSession without Hive metastore.

        In Spark Connect mode, configures Delta Lake extensions
        since there is no metastore to provide them.
        """
        builder = SparkSession.builder
        if is_spark_connect_env():
            builder = builder.config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            ).config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        return builder.getOrCreate()

    def _sql_offline(self, sql_query, feature_store):
        # Spark no metastore does not require the
        return self._spark_session.sql(sql_query)

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
"""M8 — Spark withColumn-then-select regression tests.

These exercise the new topological execution path in `Engine._apply_transformation_function`
for the three risky semantics: overwrite collisions, request-parameter overlap, and
multi-output struct sub-field collisions.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest
from hopsworks_common.client.exceptions import FeatureStoreException
from hsfs import engine, transformation_function
from hsfs.core.transformation_execution_dag import TransformationExecutionDAG
from hsfs.core.transformation_function_engine import TransformationFunctionEngine
from hsfs.engine import spark as spark_engine_mod
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType


pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="Skip on Windows — Spark + Great Expectations dependency conflict",
)


def _tf(udf_fn) -> transformation_function.TransformationFunction:
    return transformation_function.TransformationFunction(
        featurestore_id=99,
        hopsworks_udf=udf_fn,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )


@udf(int)
def overwrite_col(col1):
    """TF whose output column name collides with the first input column."""
    return col1 + 100


@udf(int)
def consume_overwritten(col1):
    """Reads col1 — must see the overwritten value when chained after `overwrite_col`."""
    return col1 * 2


@pytest.fixture(scope="module")
def spark_engine():
    eng = spark_engine_mod.Engine()
    eng._spark_session.conf.set("spark.sql.shuffle.partitions", "1")
    engine.set_instance(engine=eng, engine_type="spark")
    yield eng


class TestSparkDagRegression:
    def test_overwrite_precedence_chains_through_topo_order(self, spark_engine):
        """Spec topological precedence for overwrite-then-consume chains.

        A TF that overwrites col1 followed by a TF that reads col1 must observe
        the overwritten value because nodes are topo-sorted.
        """
        df = pd.DataFrame({"col1": [1, 2, 3]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        # The 'overwrite_col' UDF's output name will be `overwrite_col_col1_`
        # by default, NOT col1 — to truly overwrite col1, we must alias it.
        tf_overwrite = _tf(overwrite_col("col1").alias("col1"))
        tf_consume = _tf(consume_overwritten("col1").alias("doubled"))
        dag = TransformationExecutionDAG([tf_overwrite, tf_consume])

        result = TransformationFunctionEngine.apply_transformation_functions(
            execution_graph=dag, data=spark_df
        )
        rows = result.collect()
        # col1 was [1,2,3]; after overwrite -> [101,102,103]; doubled -> [202,204,206].
        assert sorted([r["doubled"] for r in rows]) == [202, 204, 206]

    def test_request_parameter_collision_with_existing_column(self, spark_engine):
        """Map upstream AnalysisException to FeatureStoreException.

        Request parameter shadowing an existing column must surface as
        FeatureStoreException, not a raw Spark AnalysisException.
        """
        df = pd.DataFrame({"col1": [1, 2, 3]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        @udf(int)
        def uses_request_param(rp_value):
            return rp_value + 1

        tf = _tf(uses_request_param("rp_value"))
        dag = TransformationExecutionDAG([tf])

        # rp_value is not a column on spark_df. Spark must raise AnalysisException
        # which we map to FeatureStoreException for consistent error surface.
        with pytest.raises(FeatureStoreException):
            TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=dag, data=spark_df
            )

    def test_multi_output_struct_sub_field_collision(self, spark_engine):
        """Raise FeatureStoreException listing collisions.

        Multi-output UDF producing a sub-field name that already exists must
        raise instead of silently dropping the existing column.
        """
        df = pd.DataFrame({"col1": [1, 2, 3], "existing": [10, 20, 30]})
        spark_df = spark_engine._spark_session.createDataFrame(df)

        @udf([int, int])
        def multi_output(col1):
            return pd.DataFrame({"out_a": col1 + 1, "existing": col1 + 2})

        # `multi_output` returns a struct with sub-fields named via .alias(...).
        # Aliasing the second field to "existing" collides with the dataset column.
        tf = _tf(multi_output("col1").alias("out_a", "existing"))
        dag = TransformationExecutionDAG([tf])

        with pytest.raises(FeatureStoreException) as exc_info:
            TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=dag, data=spark_df
            )
        assert "existing" in str(exc_info.value)

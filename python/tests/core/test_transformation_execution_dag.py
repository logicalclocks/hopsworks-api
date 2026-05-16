#
#   Copyright 2022 Hopsworks AB
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

import pytest
from hopsworks_common.client import exceptions
from hsfs import transformation_function
from hsfs.core import transformation_execution_dag
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType


class TestTransformationExecutionDAG:
    def test_build_transformation_function_execution_graph_no_dependencies(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf1, tf2]
        )

        assert tf1 in execution_graph.nodes and tf2 in execution_graph.nodes
        assert len(execution_graph.nodes) == 2
        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []

    def test_build_transformation_function_execution_graph_with_dependencies(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add_three(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf1, tf2, tf3]
        )

        assert len(execution_graph.nodes) == 3
        assert all(tf in execution_graph.nodes for tf in [tf1, tf2, tf3])
        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: roots before dependents
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

    def test_build_transformation_function_execution_graph_with_dependencies_aliased(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1.alias("odt1")
        tf2 = tf2("odt1").alias("odt2")
        tf3 = tf3("odt1", "odt2")

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf1, tf2, tf3]
        )

        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == [tf1]
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: tf1 -> tf2 -> tf3
        assert execution_graph.nodes.index(tf1) < execution_graph.nodes.index(tf2)
        assert execution_graph.nodes.index(tf2) < execution_graph.nodes.index(tf3)

    def test_build_transformation_function_execution_graph_with_overwrite_feature(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def col1(col1, col2):
            return col1 + col2

        @udf(int)
        def col2(col2):
            return col1 + 2

        @udf(int)
        def add(col1, col2):
            return col1 + col2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=col1,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=col2,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf1, tf2, tf3]
        )

        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: roots before dependents
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

    def test_build_transformation_function_execution_graph_with_dependencies_cyclic_dependency(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1, add_three):
            return col1 + 1

        @udf(int)
        def add_two(add_one):
            return add_one + 2

        @udf(int)
        def add_three(add_two, add_one):
            return add_two + add_one

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with pytest.raises(exceptions.TransformationFunctionException) as e_info:
            _ = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2, tf3])

        assert (
            str(e_info.value)
            == "Cyclic dependency detected in transformation functions."
        )

    def test_visualize_empty_dag(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")
        dag = transformation_execution_dag.TransformationExecutionDAG()
        dot = dag._to_graphviz()
        # Empty DAG should produce a valid Digraph with no nodes
        assert "Input Features" not in dot.source
        assert "Output Features" not in dot.source

    def test_visualize_independent_tfs(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        dot = dag._to_graphviz()

        # Verify Input and Output nodes exist
        assert "Input Features" in dot.source
        assert "Output Features" in dot.source
        # Verify TF node labels
        assert "add_one" in dot.source
        assert "add_two" in dot.source
        assert "mode: default" in dot.source
        # Verify edge labels contain feature names
        assert "col1" in dot.source

    def test_visualize_diamond_dependency(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add_three(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2, tf3])
        dot = dag._to_graphviz()

        # Verify all TF nodes
        assert "add_one" in dot.source
        assert "add_two" in dot.source
        assert "add_three" in dot.source
        # Only add_three's output goes to Output node (add_one and add_two are consumed)
        assert "Output Features" in dot.source
        # TF->TF edges should have linking column labels
        source = dot.source
        # add_one -> add_three edge and add_two -> add_three edge exist
        assert str(id(tf1)) in source
        assert str(id(tf3)) in source

    def test_visualize_aliased_multi_output(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf([float, float])
        def add_one(feature):
            return feature + 1, feature + 1

        @udf([float, float])
        def add_two(feature):
            return feature + 2, feature + 2

        @udf(float)
        def add(feature1, feature2):
            return feature1 + feature2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("feature").alias("odt1_1", "odt1_2")
        tf2 = tf2("feature").alias("odt2_1", "odt2_2")
        tf3 = tf3("odt1_1", "odt2_2")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2, tf3])
        dot = dag._to_graphviz()

        source = dot.source
        # Verify TF nodes
        assert "add_one" in source
        assert "add_two" in source
        # Terminal outputs (not consumed by downstream): odt1_2, odt2_1, add
        assert "odt1_2" in source
        assert "odt2_1" in source
        # Intermediate outputs on TF->TF edges: odt1_1, odt2_2
        assert "odt1_1" in source
        assert "odt2_2" in source

    def test_visualize_dropped_features(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def min_max_scaler(amount):
            return amount * 2

        @udf(float, drop=["category"])
        def label_encoder(category):
            return category

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=label_encoder,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        dot = dag._to_graphviz()

        source = dot.source
        assert "drops: amount" in source
        assert "drops: category" in source
        assert "#EA5556" in source  # Red color for dropped features

    def test_visualize_orient_parameter(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1])

        # Default is TB
        dot_tb = dag._to_graphviz()
        assert "rankdir=TB" in dot_tb.source

        # LR orientation
        dot_lr = dag._to_graphviz(orient="LR")
        assert "rankdir=LR" in dot_lr.source

    def test_visualize_pass_through_features(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def scale_amount(amount, price):
            return amount / price

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=scale_amount,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1])
        dot = dag._to_graphviz()
        source = dot.source

        # price is not dropped -> should have a dashed pass-through edge to output
        assert "price" in source
        assert "dashed" in source
        # amount IS dropped -> should NOT appear in pass-through edge
        # (it still appears in Input->TF edge and drops label, but not in pass-through)

        text = str(dag)
        # Pass-through features in text output
        assert "price" in text
        assert "scale_amount" in text
        # Output should include both price (pass-through) and scale_amount (TF output)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        assert "price" in output_line[0]
        assert "scale_amount" in output_line[0]

    def test_visualize_no_pass_through_when_all_dropped(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def min_max_scaler(amount):
            return amount * 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1])
        dot = dag._to_graphviz()
        source = dot.source

        # amount is dropped -> no dashed pass-through edge
        assert "dashed" not in source

        text = str(dag)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        # Only TF output, no pass-through
        assert "min_max_scaler" in output_line[0]
        assert "amount" not in output_line[0]

    def test_visualize_dropped_intermediate_output(self, mocker):
        """Intermediate TF output dropped by downstream TF should NOT appear in Output edges."""
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float)
        def step1(raw_col):
            return raw_col * 2

        @udf(float, drop=["step1"])
        def step2(step1):
            return step1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=step1,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=step2,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("raw_col")
        tf2 = tf2("step1")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        dot = dag._to_graphviz()
        source = dot.source

        # step2 output should map to Output
        assert 'output [label="step2"]' in source or "step2" in source
        # step1 is dropped by step2 -> should NOT have a TF edge to output
        # Filter TF->output edges (exclude input->output pass-through)
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "-> output" in line and "input" not in line
        ]
        # Should have exactly 1 TF->output edge (from step2 TF node)
        assert len(tf_output_edges) == 1
        # The edge label should be "step2" not "step1"
        assert "step2" in tf_output_edges[0]
        assert "step1" not in tf_output_edges[0]

        # Text output should also exclude step1 from Output Features
        text = str(dag)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        assert "step2" in output_line[0]
        assert "step1" not in output_line[0]

    def test_visualize_single_edge_per_tf_to_output(self, mocker):
        """Multiple output columns from one TF should produce a single edge to Output."""
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf([float, float])
        def multi_out(col1):
            return col1 + 1, col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=multi_out,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf1 = tf1("col1").alias("out_a", "out_b")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1])
        dot = dag._to_graphviz()
        source = dot.source

        # Should have exactly 1 TF->output edge (not 2 separate edges)
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "-> output" in line and "input" not in line
        ]
        assert len(tf_output_edges) == 1
        # Both column names on the single edge label
        assert "out_a" in tf_output_edges[0]
        assert "out_b" in tf_output_edges[0]

    def test_visualize_graphviz_not_installed(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        dag = transformation_execution_dag.TransformationExecutionDAG()

        mocker.patch.dict("sys.modules", {"graphviz": None})

        with pytest.raises(ImportError, match="pip install graphviz"):
            dag._to_graphviz()

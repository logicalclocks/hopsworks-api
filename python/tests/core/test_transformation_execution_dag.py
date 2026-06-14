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

import warnings

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
            return col2 + 2

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

        # tf1 (`col1(col1, col2)` → output `col1`) reads two inputs.
        # The `col1` input is produced by tf1 itself, so the self-loop is
        # filtered out (a TF that overwrites one of its own inputs is treated
        # as an identity edge on that column).
        # The `col2` input is produced by a different TF (tf2), so tf2 IS a
        # genuine predecessor — the self-loop on one input does not erase
        # dependencies on the other inputs.
        assert execution_graph.dependencies[id(tf1)] == [tf2]
        # tf2 (`col2(col2)` → output `col2`) only has the self-loop, so no
        # real predecessors remain after filtering.
        assert execution_graph.dependencies[id(tf2)] == []
        # tf3 (`add(col1, col2)`) consumes both upstream outputs.
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Full topological order: tf2 → tf1 → tf3.
        assert execution_graph.nodes.index(tf2) < execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf1) < execution_graph.nodes.index(tf3)

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

        # Building the DAG rejects a cyclic configuration up front.
        with pytest.raises(
            exceptions.TransformationFunctionException,
            match="Cyclic dependency detected in transformation functions",
        ) as exc:
            transformation_execution_dag.TransformationExecutionDAG([tf1, tf2, tf3])

        # The error names a function on the cycle and the remediation.
        assert "add_one" in str(exc.value)
        assert ".alias()" in str(exc.value)

    def test_build_transformation_function_execution_graph_ambiguous_schema_feature_warns(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def producer(col1):
            return col1 + 1

        @udf(int)
        def consumer(imputed):
            return imputed + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=producer("col1").alias("imputed"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=consumer,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        # 'imputed' is both a raw schema feature and tf1's output: warn, but
        # still chain (the edge is created and the topo order reflects it).
        with pytest.warns(UserWarning, match="both a feature of this entity"):
            execution_graph = transformation_execution_dag.TransformationExecutionDAG(
                [tf1, tf2], schema_feature_names={"col1", "imputed"}
            )

        assert execution_graph.dependencies[id(tf2)] == [tf1]
        assert execution_graph.nodes.index(tf1) < execution_graph.nodes.index(tf2)

        # Without the name collision in the schema there is no warning.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            execution_graph = transformation_execution_dag.TransformationExecutionDAG(
                [tf1, tf2], schema_feature_names={"col1"}
            )
        assert execution_graph.dependencies[id(tf2)] == [tf1]

    def test_execution_dag_rejects_aliased_cycle(self, mocker):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def f_a(out_b):
            return out_b + 1

        @udf(int)
        def f_b(out_a):
            return out_a + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=f_a("out_b").alias("out_a"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=f_b("out_a").alias("out_b"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        # A two-node cycle formed via aliases is rejected at construction.
        with pytest.raises(
            exceptions.TransformationFunctionException,
            match="Cyclic dependency detected",
        ):
            transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

    def test_visualize_empty_dag(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")
        dag = transformation_execution_dag.TransformationExecutionDAG()
        src = dag._to_mermaid()
        # Empty DAG should produce a valid Digraph with no nodes
        assert "Input Features" not in src
        assert "Output Features" not in src

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
        src = dag._to_mermaid()

        # Verify Input and Output nodes exist
        assert "Input Features" in src
        assert "Output Features" in src
        # Verify TF node labels
        assert "add_one" in src
        assert "add_two" in src
        # Verify edge labels contain feature names
        assert "col1" in src

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
        src = dag._to_mermaid()

        # Verify all TF nodes
        assert "add_one" in src
        assert "add_two" in src
        assert "add_three" in src
        # Only add_three's output goes to Output node (add_one and add_two are consumed)
        assert "Output Features" in src
        # TF->TF edges should have linking column labels
        source = src
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
        src = dag._to_mermaid()

        source = src
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
        source = dag._to_mermaid()

        # The diagram deliberately omits drop metadata to keep the flow clean
        # — drop info still surfaces through the text representation.
        assert "drops:" not in source
        text = str(dag)
        assert "drops: amount" in text
        assert "drops: category" in text

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

        # Both orientations end up in the `flowchart <orient>` header.
        assert "flowchart TB" in dag._to_mermaid(orient="TB")
        assert "flowchart LR" in dag._to_mermaid(orient="LR")

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
        source = dag._to_mermaid()

        # The Mermaid diagram intentionally omits the Input -> Output
        # pass-through edge to keep the flow uncluttered. The information
        # remains available in the text representation.
        assert "-.->|" not in source

        text = str(dag)
        # Text output keeps the pass-through info visible.
        assert "price" in text
        assert "scale_amount" in text
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
        source = dag._to_mermaid()

        # Mermaid never draws an Input -> Output pass-through edge in any case.
        assert "-.->|" not in source

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
        src = dag._to_mermaid()
        source = src

        # step2 output should map to Output
        assert 'output [label="step2"]' in source or "step2" in source
        # step1 is dropped by step2 -> should NOT have a TF edge to output
        # Filter TF->output edges (exclude input->output pass-through)
        # Mermaid edges land in `| output` after the label pipe.
        # Exclude `input` lines (which match the same `| output` pattern for
        # pass-through edges).
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "| output" in line and "input" not in line
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
        src = dag._to_mermaid()
        source = src

        # Should have exactly 1 TF->output edge (not 2 separate edges)
        # Mermaid edges land in `| output` after the label pipe.
        # Exclude `input` lines (which match the same `| output` pattern for
        # pass-through edges).
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "| output" in line and "input" not in line
        ]
        assert len(tf_output_edges) == 1
        # Both column names on the single edge label
        assert "out_a" in tf_output_edges[0]
        assert "out_b" in tf_output_edges[0]

    def test_visualize_invalid_mode_raises(self, mocker):
        """Modes outside the supported set must be rejected with a clear error."""
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        dag = transformation_execution_dag.TransformationExecutionDAG()
        with pytest.raises(ValueError, match="Invalid mode"):
            dag._visualize(mode="graph")  # unsupported mode must error explicitly

    def test_visualize_auto_uses_mermaid_inside_jupyter(self, mocker):
        """The default `auto` mode picks Mermaid when running in Jupyter."""
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )("feature")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf])

        mocker.patch.object(
            transformation_execution_dag.TransformationExecutionDAG,
            "_in_jupyter",
            staticmethod(lambda: True),
        )
        mermaid_spy = mocker.patch.object(dag, "_display_mermaid")

        dag._visualize(mode="auto", orient="LR")

        mermaid_spy.assert_called_once_with(orient="LR")

    def test_visualize_auto_uses_text_outside_jupyter(self, mocker, capsys):
        """Outside Jupyter `auto` must print the text representation."""
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )("feature")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf])
        mocker.patch.object(
            transformation_execution_dag.TransformationExecutionDAG,
            "_in_jupyter",
            staticmethod(lambda: False),
        )

        dag._visualize(mode="auto", orient="LR")

        out = capsys.readouterr().out
        assert "Transformation Execution DAG" in out
        assert "add_one" in out

    def test_visualize_mermaid_emits_styled_flowchart(self, mocker):
        """The Mermaid output carries the styled palette + correct shapes."""
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf([float, float])
        def add_one(feature):
            return feature + 1, feature + 1

        @udf(float, drop="feature1")
        def add(feature1, feature2):
            return feature1 + feature2

        tf1 = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )("feature").alias("odt1_1", "odt1_2")
        tf2 = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )("odt1_1", "odt1_2")

        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        src = dag._to_mermaid(orient="LR")

        # Header + stadium-shaped IO anchors with their styling classes.
        assert src.startswith("flowchart LR")
        assert 'input(["<b>Input Features</b>"]):::inputAnchor' in src
        assert 'output(["<b>Output Features</b>"]):::outputAnchor' in src
        # Every TF uses the same `tf` class — no per-TF mode/drops markings
        # so the canvas stays uncluttered.
        assert ":::tf" in src
        assert ":::tfDrop" not in src
        assert "drops:" not in src
        # Node labels show just the function name.
        assert "<b>add</b>" in src
        # Edge label combines the columns that flow from tf1 into tf2.
        assert '|"odt1_1, odt1_2"|' in src
        # The three styling classes still in use after the simplification.
        for cls in ("inputAnchor", "outputAnchor", "tf"):
            assert f"classDef {cls}" in src

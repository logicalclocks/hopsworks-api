#
#   Copyright 2021 Logical Clocks AB
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

from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from hopsworks_common.client import exceptions


if TYPE_CHECKING:
    import graphviz
    from hsfs import transformation_function


class TransformationExecutionDAG:
    """Execution DAG for transformation functions.

    Analyzes dependencies between transformation functions by inspecting their input and output features,
    then topologically sorts them so that every function runs only after the functions it depends on have completed.

    Parameters:
        transformation_functions: Flat list of transformation functions to organize into a DAG.
            When ``None`` or empty, the DAG is empty (no nodes, no dependencies).

    Raises:
        TransformationFunctionException: If a cyclic dependency is detected among the transformation functions.
    """

    def __init__(
        self,
        transformation_functions: list[transformation_function.TransformationFunction]
        | None = None,
    ):
        self._nodes: list[transformation_function.TransformationFunction] = []
        self._dependencies: dict[
            int, list[transformation_function.TransformationFunction]
        ] = {}

        if not transformation_functions:
            return

        # Map each output column to the TF that produces it; raise on duplicates
        # to surface conflicting transformations early instead of silently dropping one.
        map_output_to_tfs: dict[str, transformation_function.TransformationFunction] = {}
        for tf in transformation_functions:
            for col in tf.hopsworks_udf.output_column_names:
                existing = map_output_to_tfs.get(col)
                if existing is not None and existing is not tf:
                    raise exceptions.FeatureStoreException(
                        f"Output column '{col}' is produced by both "
                        f"'{existing.hopsworks_udf.function_name}' and "
                        f"'{tf.hopsworks_udf.function_name}'."
                    )
                map_output_to_tfs[col] = tf

        # For each TF, find predecessors (TFs whose outputs are this TF's inputs).
        # A TF that overwrites one of its own inputs (self-loop) is filtered by
        # producer-identity: the input is skipped only when its producer is `tf`
        # itself, preserving edges from any other TF that produces a different input.
        deps: dict[int, list] = {}
        for tf in transformation_functions:
            features = tf.hopsworks_udf.transformation_features
            seen: set[int] = set()
            preds: list = []
            for feat in features:
                producer = map_output_to_tfs.get(feat)
                if (
                    producer is not None
                    and producer is not tf
                    and id(producer) not in seen
                ):
                    preds.append(producer)
                    seen.add(id(producer))
            deps[id(tf)] = preds

        # Topological sort with cycle detection
        id_to_tf = {id(tf): tf for tf in transformation_functions}
        try:
            self._nodes = [
                id_to_tf[tid]
                for tid in TopologicalSorter(
                    {
                        id(tf): [id(p) for p in deps[id(tf)]]
                        for tf in transformation_functions
                    }
                ).static_order()
            ]
        except CycleError:
            raise exceptions.TransformationFunctionException(
                "Cyclic dependency detected in transformation functions."
            ) from None

        self._dependencies = deps

    @property
    def nodes(self) -> list[transformation_function.TransformationFunction]:
        """Transformation functions in topological execution order.

        Each function appears after all of its predecessors, so iterating
        over this list guarantees that every dependency has already been
        processed.
        """
        return self._nodes

    @property
    def dependencies(
        self,
    ) -> dict[int, list[transformation_function.TransformationFunction]]:
        """Per-node predecessor map.

        Keys are ``id(tf)`` for each transformation function in the DAG.
        Values are lists of transformation functions that must complete
        before the keyed function can execute.
        An empty list means the function has no dependencies and can start
        immediately.
        """
        return self._dependencies

    def _compute_depths(self) -> dict[int, int]:
        """Compute the depth (longest path from any root) for each node."""
        depths: dict[int, int] = {}
        for tf in self.nodes:
            deps = self.dependencies.get(id(tf), [])
            depths[id(tf)] = max(depths[id(dep)] for dep in deps) + 1 if deps else 0
        return depths

    @property
    def max_parallelism(self) -> int:
        """Maximum number of transformations that can execute concurrently.

        Returns the largest number of nodes sharing the same depth level,
        which equals the maximum width of the DAG schedule.
        """
        if not self.nodes:
            return 1
        level_counts: dict[int, int] = {}
        for d in self._compute_depths().values():
            level_counts[d] = level_counts.get(d, 0) + 1
        return max(level_counts.values())

    @staticmethod
    def _in_jupyter() -> bool:
        """Detect if running inside a Jupyter notebook."""
        try:
            from IPython import get_ipython

            shell = get_ipython()
            if shell is None:
                return False
            return shell.__class__.__name__ in (
                "ZMQInteractiveShell",
                "Shell",
            )
        except ImportError:
            return False

    def _build_lookups(
        self,
    ) -> tuple[
        dict[str, transformation_function.TransformationFunction],
        set[str],
        dict[int, list[str]],
        list[str],
    ]:
        """Build shared lookups for visualization.

        Returns:
            A tuple of (output_to_tf, raw_inputs, tf_outputs, pass_through_features).
        """
        # Pass 1: map output columns to producing TFs and collect dropped features.
        output_to_tf: dict[str, transformation_function.TransformationFunction] = {}
        all_dropped: set[str] = set()
        for tf in self.nodes:
            udf = tf.hopsworks_udf
            for col in udf.output_column_names:
                output_to_tf[col] = tf
            if udf.dropped_features:
                all_dropped.update(udf.dropped_features)

        # Pass 2: identify raw inputs and non-dropped TF outputs.
        raw_inputs: set[str] = set()
        tf_outputs: dict[int, list[str]] = {}
        for tf in self.nodes:
            udf = tf.hopsworks_udf
            for feat in udf.transformation_features:
                if feat not in output_to_tf:
                    raw_inputs.add(feat)
            cols = [c for c in udf.output_column_names if c not in all_dropped]
            if cols:
                tf_outputs[id(tf)] = cols

        return output_to_tf, raw_inputs, tf_outputs, sorted(raw_inputs - all_dropped)

    def __str__(self) -> str:
        """Text representation of the DAG for terminal display."""
        if not self.nodes:
            return "Transformation Execution DAG (empty)"

        arrow = "       \u2502\n       \u25bc"
        _, raw_inputs, tf_outputs, pass_through_features = self._build_lookups()

        lines = ["Transformation Execution DAG", "\u2550" * 35, ""]

        if raw_inputs:
            lines.append(f"Input Features: {', '.join(sorted(raw_inputs))}")
            lines.append(arrow)

        depths = self._compute_depths()
        levels: dict[int, list] = {}
        for tf in self.nodes:
            levels.setdefault(depths[id(tf)], []).append(tf)

        max_level = max(levels)
        for level_idx in sorted(levels):
            for tf in levels[level_idx]:
                udf = tf.hopsworks_udf
                header = f"  {udf.function_name} (mode: {udf.execution_mode.value})"
                if udf.dropped_features:
                    header += f"  [drops: {', '.join(udf.dropped_features)}]"
                lines.append(header)
            if level_idx < max_level:
                lines.append(arrow)

        all_output_cols = pass_through_features + [
            col for cols in tf_outputs.values() for col in cols
        ]
        if all_output_cols:
            lines.append(arrow)
            lines.append(f"Output Features: {', '.join(all_output_cols)}")

        return "\n".join(lines)

    def visualize(self, mode: str = "auto", orient: str = "TB") -> None:
        """Display the transformation execution DAG.

        Renders the DAG inline in the current environment:

        - `"auto"` (default): graphviz SVG in Jupyter, text in terminals.
        - `"text"`: always prints a text representation to stdout.
        - `"graph"`: always renders a graphviz SVG (Jupyter only).

        Example:
            ```python
            # Auto-detect environment
            fg._transformation_function_execution_dag.visualize()

            # Force text output in any environment
            fg._transformation_function_execution_dag.visualize(mode="text")
            ```

        Parameters:
            mode: Display mode. One of `"auto"` (default), `"text"`, `"graph"`.
            orient: Layout direction for the graphviz graph. One of:
                `"TB"` (top-to-bottom, default), `"LR"` (left-to-right),
                `"BT"` (bottom-to-top), `"RL"` (right-to-left).
                Only used when rendering as graphviz.

        Raises:
            `ImportError`: If mode is `"graph"` and the `graphviz` package
                is not installed.
            `ValueError`: If `mode` is not one of `"auto"`, `"text"`, `"graph"`.
        """
        valid_modes = ("auto", "text", "graph")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of {valid_modes}.")

        if mode == "text" or (mode == "auto" and not self._in_jupyter()):
            print(self)
            return

        # mode == "graph" or (mode == "auto" and in Jupyter)
        from IPython.display import display

        display(self._to_graphviz(orient=orient))

    def _to_graphviz(self, orient: str = "TB") -> graphviz.Digraph:
        """Build a graphviz.Digraph representation of the DAG.

        Parameters:
            orient: Layout direction. One of `"TB"`, `"LR"`, `"BT"`, `"RL"`.

        Returns:
            A `graphviz.Digraph` object.

        Raises:
            `ImportError`: If the `graphviz` package is not installed.
        """
        try:
            import graphviz
        except ImportError as e:
            raise ImportError(
                "The 'graphviz' package is required for visualization. "
                "Install it with: pip install hopsworks[viz]"
            ) from e

        output_to_tf, raw_inputs, tf_outputs, pass_through_features = (
            self._build_lookups()
        )

        dot = graphviz.Digraph(
            comment="Transformation Function Execution DAG",
            graph_attr={
                "rankdir": orient,
                "ranksep": "0.6",
            },
            node_attr={
                "shape": "rectangle",
                "style": "rounded,filled",
                "fillcolor": "#b4d8e4",
                "fontname": "Helvetica",
                "margin": "0.15",
            },
            edge_attr={"fontname": "Helvetica", "fontsize": "9"},
        )

        io_style = {"shape": "rectangle", "style": "filled", "margin": "0.2"}
        if raw_inputs:
            dot.node(
                "input", "<<b>Input Features</b>>", fillcolor="#E8F4FD", **io_style
            )

        for tf in self.nodes:
            udf = tf.hopsworks_udf
            label = f"<<b>{udf.function_name}</b><br/><i>mode: {udf.execution_mode.value}</i>"
            if udf.dropped_features:
                label += f'<br/><font color="#EA5556">drops: {", ".join(udf.dropped_features)}</font>'
            dot.node(str(id(tf)), label + ">")

        if tf_outputs or pass_through_features:
            dot.node(
                "output", "<<b>Output Features</b>>", fillcolor="#D4EDDA", **io_style
            )

        # Edges: Input -> TF
        input_edges: dict[int, list[str]] = {}
        for tf in self.nodes:
            for feat in tf.hopsworks_udf.transformation_features:
                if feat not in output_to_tf:
                    input_edges.setdefault(id(tf), []).append(feat)
        for tf_id, feats in input_edges.items():
            dot.edge("input", str(tf_id), label=", ".join(feats))

        # Edges: Input -> Output (pass-through features not dropped by any TF)
        if pass_through_features:
            dot.edge(
                "input",
                "output",
                label=", ".join(pass_through_features),
                style="dashed",
            )

        # Edges: TF -> TF
        for tf in self.nodes:
            for dep in self.dependencies.get(id(tf), []):
                linking = [
                    col
                    for col in dep.hopsworks_udf.output_column_names
                    if col in tf.hopsworks_udf.transformation_features
                ]
                dot.edge(str(id(dep)), str(id(tf)), label=", ".join(linking))

        # Edges: TF -> Output (one edge per TF with all non-dropped outputs)
        for tf_id, cols in tf_outputs.items():
            dot.edge(str(tf_id), "output", label=", ".join(cols))

        return dot

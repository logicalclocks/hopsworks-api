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
from __future__ import annotations

import warnings
from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING

from hopsworks_common.client import exceptions


if TYPE_CHECKING:
    from hsfs import transformation_function


class TransformationExecutionDAG:
    """Execution DAG for transformation functions.

    Analyzes dependencies between transformation functions by inspecting their input and output features,
    then topologically sorts them so that every function runs only after the functions it depends on have completed.

    Construction validates the configuration: a duplicate output column or a
    cyclic dependency raises immediately, so an invalid configuration is rejected
    the moment its DAG is built.

    Parameters:
        transformation_functions: Flat list of transformation functions to organize into a DAG.
            When ``None`` or empty, the DAG is empty (no nodes, no dependencies).
        schema_feature_names: Raw feature names of the owning entity, when known.
            Used to warn when a consumed feature name exists both as a raw feature and as another transformation function's output, since the name then resolves to the transformation output (chained execution) rather than the raw feature.
            Pass ``None`` to skip the ambiguity check.

    Raises:
        FeatureStoreException: If two transformation functions produce the same output column.
        TransformationFunctionException: If the transformation functions form a cyclic dependency.
    """

    def __init__(
        self,
        transformation_functions: list[transformation_function.TransformationFunction]
        | None = None,
        schema_feature_names: set[str] | None = None,
    ):
        self._nodes: list[transformation_function.TransformationFunction] = []
        self._dependencies: dict[
            int, list[transformation_function.TransformationFunction]
        ] = {}
        # Adjacency keyed by id(transformation_function), the form graphlib's
        # TopologicalSorter consumes. Built once here and reused for the static
        # order below and for every streaming sorter handed out at execution.
        self._id_dependencies: dict[int, list[int]] = {}

        if not transformation_functions:
            return

        # Map each output column to the TF that produces it; raise on duplicates
        # to surface conflicting transformations early instead of silently dropping one.
        map_output_to_tfs: dict[
            str, transformation_function.TransformationFunction
        ] = {}
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
                if producer is None or producer is tf:
                    continue
                # Ambiguous name: the consumed feature exists both as a raw
                # feature of the entity and as another TF's output. The name
                # resolves to the transformation output (chaining), so warn in
                # case the raw feature was intended.
                if schema_feature_names and feat in schema_feature_names:
                    warnings.warn(
                        f"Feature '{feat}' consumed by transformation function "
                        f"'{tf.hopsworks_udf.function_name}' is both a feature of this entity "
                        f"and an output of transformation function "
                        f"'{producer.hopsworks_udf.function_name}'. It resolves to the "
                        "transformation output (chained execution). If the raw feature was "
                        "intended, rename the output with `.alias()`.",
                        stacklevel=2,
                    )
                if id(producer) not in seen:
                    preds.append(producer)
                    seen.add(id(producer))
            deps[id(tf)] = preds

        self._id_dependencies = {
            id(tf): [id(p) for p in deps[id(tf)]] for tf in transformation_functions
        }

        # Topological sort with cycle detection. A cycle has no valid execution
        # order, so it is rejected here rather than producing an unusable DAG.
        id_to_tf = {id(tf): tf for tf in transformation_functions}
        try:
            self._nodes = [
                id_to_tf[tid]
                for tid in TopologicalSorter(self._id_dependencies).static_order()
            ]
        except CycleError as e:
            cycle_ids = e.args[1] if len(e.args) > 1 else []
            cycle_path = " -> ".join(
                id_to_tf[tf_id].hopsworks_udf.function_name
                for tf_id in cycle_ids
                if tf_id in id_to_tf
            )
            raise exceptions.TransformationFunctionException(
                "Cyclic dependency detected in transformation functions"
                + (f": {cycle_path}" if cycle_path else "")
                + ". Chaining is inferred from output/input column names; rename outputs"
                " with `.alias()` to break unintended cycles."
            ) from e

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

    def _new_topological_sorter(self) -> TopologicalSorter:
        """Return a fresh, prepared topological sorter keyed by ``id(tf)``.

        Callers that drive a streaming schedule (submitting a transformation as
        soon as its predecessors complete) use this instead of rebuilding the
        dependency graph.

        Returns:
            A prepared, single-use ``TopologicalSorter``. A new instance is
            returned on each call because a prepared sorter cannot be reused.
        """
        sorter = TopologicalSorter(self._id_dependencies)
        sorter.prepare()
        return sorter

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

    def _visualize(self, mode: str = "auto", orient: str = "LR") -> None:
        """Display the transformation execution DAG.

        Modes:

        - `"auto"` (default): render a Mermaid `flowchart` inline when running
          inside a Jupyter frontend, otherwise print the text representation.
        - `"text"`: always print a text representation to stdout.
        - `"mermaid"`: always emit a Mermaid `flowchart` block, rendered inline
          in Mermaid-aware Jupyter clients and printed as source otherwise.

        Parameters:
            mode: Display mode. One of `"auto"` (default), `"text"`, `"mermaid"`.
            orient: Layout direction for the rendered flowchart. One of:
                `"LR"` (left-to-right, default), `"TB"` (top-to-bottom),
                `"BT"` (bottom-to-top), `"RL"` (right-to-left).

        Raises:
            `ValueError`: If `mode` is not one of `"auto"`, `"text"`, `"mermaid"`.
        """
        valid_modes = ("auto", "text", "mermaid")
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of {valid_modes}.")

        if mode == "text":
            print(self)
            return

        if mode == "mermaid":
            self._display_mermaid(orient=orient)
            return

        # auto: inline Mermaid inside Jupyter, plain text everywhere else.
        if self._in_jupyter():
            self._display_mermaid(orient=orient)
        else:
            print(self)

    def _display_mermaid(self, orient: str) -> None:
        """Render the DAG as a Mermaid flowchart in the active environment.

        Modern Jupyter clients render Mermaid inline via the Markdown
        mimetype; outside Jupyter we fall back to printing the fenced source
        so the output is still copy-pasteable into any Mermaid viewer.

        Parameters:
            orient: Layout direction passed through to the `flowchart` header.
        """
        source = self._to_mermaid(orient=orient)
        markdown = f"```mermaid\n{source}\n```"

        rendered_inline = False
        if self._in_jupyter():
            try:
                from IPython.display import Markdown, display

                display(Markdown(markdown))
                rendered_inline = True
            except ImportError:
                pass

        if not rendered_inline:
            print(markdown)

    def _to_mermaid(self, orient: str = "LR") -> str:
        """Build a Mermaid `flowchart` representation of the DAG.

        Input and output anchors are stadium-shaped; transformation functions
        are rounded rectangles labelled with the function name. Edges carry
        their linking feature names. Execution mode and dropped-feature
        metadata are shown only in the text representation, not the diagram.

        Parameters:
            orient: Layout direction. One of `"TB"`, `"LR"`, `"BT"`, `"RL"`.

        Returns:
            Mermaid `flowchart` source.
        """
        output_to_tf, raw_inputs, tf_outputs, _ = self._build_lookups()

        def label(text: str) -> str:
            """Escape a label so quotes survive Mermaid's parser."""
            return text.replace('"', "&quot;")

        lines: list[str] = [f"flowchart {orient}"]

        # Stadium-shaped anchors separate "boundary" nodes from the TFs at a glance.
        if raw_inputs:
            lines.append('    input(["<b>Input Features</b>"]):::inputAnchor')

        # TF nodes use rounded rectangles labelled with the function name only;
        # mode and drops are surfaced via the text representation.
        for tf in self.nodes:
            udf = tf.hopsworks_udf
            body = f"<b>{udf.function_name}</b>"
            lines.append(f'    n{id(tf)}("{label(body)}"):::tf')

        if tf_outputs:
            lines.append('    output(["<b>Output Features</b>"]):::outputAnchor')

        # Edges: Input -> TF
        input_edges: dict[int, list[str]] = {}
        for tf in self.nodes:
            for feat in tf.hopsworks_udf.transformation_features:
                if feat not in output_to_tf:
                    input_edges.setdefault(id(tf), []).append(feat)
        for tf_id, feats in input_edges.items():
            lines.append(f'    input -->|"{label(", ".join(feats))}"| n{tf_id}')

        # Edges: TF -> TF
        for tf in self.nodes:
            for dep in self.dependencies.get(id(tf), []):
                linking = [
                    col
                    for col in dep.hopsworks_udf.output_column_names
                    if col in tf.hopsworks_udf.transformation_features
                ]
                lines.append(
                    f'    n{id(dep)} -->|"{label(", ".join(linking))}"| n{id(tf)}'
                )

        # Edges: TF -> Output (one edge per TF with all non-dropped outputs)
        for tf_id, cols in tf_outputs.items():
            lines.append(f'    n{tf_id} -->|"{label(", ".join(cols))}"| output')

        # Palette mirrors the Hopsworks frontend DAG view: pale blue inputs,
        # pale green outputs, neutral TFs. The same pastel tones render well
        # against both light and dark Mermaid themes.
        lines.extend(
            [
                "    classDef inputAnchor "
                "fill:#DBEAFE,stroke:#3B82F6,stroke-width:2px,color:#1E3A8A",
                "    classDef outputAnchor "
                "fill:#D1FAE5,stroke:#10B981,stroke-width:2px,color:#064E3B",
                "    classDef tf "
                "fill:#F9FAFB,stroke:#6B7280,stroke-width:1.5px,color:#111827",
            ]
        )

        return "\n".join(lines)

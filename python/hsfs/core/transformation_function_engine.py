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

import atexit
import multiprocessing
import sys
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from multiprocessing import shared_memory
from typing import TYPE_CHECKING, Any, TypeVar

import pandas as pd
from hopsworks_common.client import exceptions
from hopsworks_common.core.constants import HAS_POLARS, HAS_PYARROW
from hsfs import (
    engine,
    feature_view,
    statistics,
    training_dataset,
    transformation_function,
)
from hsfs.core import transformation_function_api
from hsfs.hopsworks_udf import HopsworksUdf, UDFExecutionMode


if HAS_POLARS:
    import polars as pl

if HAS_PYARROW:
    import pyarrow as pa
    import pyarrow.ipc as ipc


if TYPE_CHECKING:
    import graphviz
    import pyspark.sql as spark_sql
    from hsfs import feature_view, statistics, training_dataset, transformation_function


@dataclass
class TransformationExecutionDAG:
    """DAG with per-node dependency tracking for transformation functions."""

    nodes: list[transformation_function.TransformationFunction]
    dependencies: dict[int, list[transformation_function.TransformationFunction]] = (
        field(default_factory=dict)
    )
    dependents: dict[int, list[transformation_function.TransformationFunction]] = field(
        default_factory=dict
    )
    roots: list[transformation_function.TransformationFunction] = field(
        default_factory=list
    )

    @property
    def max_parallelism(self) -> int:
        """Maximum number of transformations that can execute concurrently.

        Computes the depth (longest path from any root) for each node and
        returns the largest number of nodes sharing the same depth level.
        This equals the maximum width of the DAG schedule.
        """
        if not self.nodes:
            return 1
        # Nodes are topologically sorted, so all deps are visited first.
        depths: dict[int, int] = {}
        for tf in self.nodes:
            deps = self.dependencies.get(id(tf), [])
            depths[id(tf)] = max(depths[id(dep)] for dep in deps) + 1 if deps else 0
        level_counts: dict[int, int] = {}
        for d in depths.values():
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
        set[str],
        dict[int, list[str]],
        list[str],
    ]:
        """Build shared lookups used by both visualize() and __str__().

        Returns:
            A tuple of (output_to_tf, consumed_columns, raw_inputs,
            tf_outputs, pass_through_features).
        """
        output_to_tf: dict[str, transformation_function.TransformationFunction] = {}
        for tf in self.nodes:
            for col in tf.hopsworks_udf.output_column_names:
                output_to_tf[col] = tf

        consumed_columns: set[str] = set()
        for tf in self.nodes:
            for feat in tf.hopsworks_udf.transformation_features:
                if feat in output_to_tf:
                    consumed_columns.add(feat)

        raw_inputs: set[str] = set()
        for tf in self.nodes:
            for feat in tf.hopsworks_udf.transformation_features:
                if feat not in output_to_tf:
                    raw_inputs.add(feat)

        # Collect all explicitly dropped features first
        all_dropped: set[str] = set()
        for tf in self.nodes:
            if tf.hopsworks_udf.dropped_features:
                all_dropped.update(tf.hopsworks_udf.dropped_features)

        # TF output columns that are not dropped go to the output
        tf_outputs: dict[int, list[str]] = {}
        for tf in self.nodes:
            cols = [
                col
                for col in tf.hopsworks_udf.output_column_names
                if col not in all_dropped
            ]
            if cols:
                tf_outputs[id(tf)] = cols

        # Pass-through: raw input features not explicitly dropped by any TF
        pass_through_features = sorted(raw_inputs - all_dropped)

        return (
            output_to_tf,
            consumed_columns,
            raw_inputs,
            tf_outputs,
            pass_through_features,
        )

    def __str__(self) -> str:
        """Text representation of the DAG for terminal display."""
        if not self.nodes:
            return "Transformation Execution DAG (empty)"

        (
            output_to_tf,
            consumed_columns,
            raw_inputs,
            tf_outputs,
            pass_through_features,
        ) = self._build_lookups()

        lines = [
            "Transformation Execution DAG",
            "\u2550" * 35,
            "",
        ]

        if raw_inputs:
            lines.append(f"Input Features: {', '.join(sorted(raw_inputs))}")
            lines.append("       \u2502")
            lines.append("       \u25bc")

        # Compute depths for level grouping
        depths: dict[int, int] = {}
        for tf in self.nodes:
            deps = self.dependencies.get(id(tf), [])
            depths[id(tf)] = max(depths[id(dep)] for dep in deps) + 1 if deps else 0

        # Group TFs by depth level
        levels: dict[int, list] = {}
        for tf in self.nodes:
            levels.setdefault(depths[id(tf)], []).append(tf)

        for level_idx in sorted(levels.keys()):
            for tf in levels[level_idx]:
                udf = tf.hopsworks_udf
                header = f"  {udf.function_name} (mode: {udf.execution_mode.value})"
                if udf.dropped_features:
                    header += f"  [drops: {', '.join(udf.dropped_features)}]"
                lines.append(header)

            if level_idx < max(levels.keys()):
                lines.append("       \u2502")
                lines.append("       \u25bc")

        # Output features = all TF outputs + pass-through input features
        tf_output_cols = [col for cols in tf_outputs.values() for col in cols]
        all_output_cols = pass_through_features + tf_output_cols
        if all_output_cols:
            lines.append("       \u2502")
            lines.append("       \u25bc")
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
                "Install it with: pip install graphviz"
            ) from e

        (
            output_to_tf,
            consumed_columns,
            raw_inputs,
            tf_outputs,
            pass_through_features,
        ) = self._build_lookups()

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

        if raw_inputs:
            dot.node(
                "input",
                "<<b>Input Features</b>>",
                shape="rectangle",
                style="filled",
                fillcolor="#E8F4FD",
                margin="0.2",
            )

        # TF nodes
        for tf in self.nodes:
            udf = tf.hopsworks_udf
            label = (
                f"<<b>{udf.function_name}</b>"
                f"<br/><i>mode: {udf.execution_mode.value}</i>"
            )
            if udf.dropped_features:
                dropped = ", ".join(udf.dropped_features)
                label += f'<br/><font color="#EA5556">drops: {dropped}</font>'
            label += ">"
            dot.node(str(id(tf)), label)

        # Output node: shown if there are TF outputs or pass-through features
        has_outputs = tf_outputs or pass_through_features
        if has_outputs:
            dot.node(
                "output",
                "<<b>Output Features</b>>",
                shape="rectangle",
                style="filled",
                fillcolor="#D4EDDA",
                margin="0.2",
            )

        # Edges: Input -> TF
        input_edges: dict[int, list[str]] = {}
        for tf in self.nodes:
            for feat in tf.hopsworks_udf.transformation_features:
                if feat not in output_to_tf:
                    input_edges.setdefault(id(tf), []).append(feat)
        for tf_id, features in input_edges.items():
            dot.edge("input", str(tf_id), label=", ".join(features))

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


class TransformationFunctionEngine:
    BUILTIN_FN_NAMES = [
        "min_max_scaler",
        "standard_scaler",
        "robust_scaler",
        "label_encoder",
    ]
    AMBIGUOUS_FEATURE_ERROR = (
        "Provided feature '{}' in transformation functions is ambiguous and exists in more than one feature groups."
        "You can provide the feature with the prefix that was specified in the join."
    )
    FEATURE_NOT_EXIST_ERROR = "Provided feature '{}' in transformation functions do not exist in any of the feature groups."

    __process_pool = None

    def __init__(self, feature_store_id: int):
        self._feature_store_id = feature_store_id
        self._transformation_function_api: transformation_function_api.TransformationFunctionApi = transformation_function_api.TransformationFunctionApi(
            feature_store_id
        )
        atexit.register(TransformationFunctionEngine.shutdown_process_pool)

    def save(
        self, transformation_fn_instance: transformation_function.TransformationFunction
    ):
        """Save a transformation function into the feature store.

        Parameters:
            transformation_fn_instance: The transformation function to be saved into the feature store.
        """
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(
        self, name: str, version: int | None = None
    ) -> (
        transformation_function.TransformationFunction
        | list[transformation_function.TransformationFunction]
    ):
        """Retrieve a transformation function from the feature store.

        If only the name of the transformation function is provided then all the versions of the transformation functions are returned as a list.
        If both name and version are not provided then all transformation functions saved in the feature view is returned.

        Parameters:
            name: The name of the transformation function to be retrieved.
            version: The version of the transformation function to be retrieved.

        Returns:
            A transformation function if name and version is provided.
            A list of transformation functions if only name is provided.
        """
        return self._transformation_function_api.get_transformation_fn(name, version)

    def get_transformation_fns(
        self,
    ) -> list[transformation_function.TransformationFunction]:
        """Get all the transformation functions in the feature store.

        Returns:
            A list of transformation functions.
        """
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(
                name=None, version=None
            )
        )
        transformation_fns = []
        for (
            transformation_fn_instance
        ) in transformation_fn_instances:  # todo what is the point of this?
            transformation_fns.append(transformation_fn_instance)
        return transformation_fns

    @staticmethod
    def shutdown_process_pool():
        """Shut down the process pool used for parallel execution of transformation functions.

        Waits for all running tasks to complete before shutting down.
        Safe to call even if no process pool has been created.
        """
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.__process_pool.shutdown(wait=True)
            TransformationFunctionEngine.__process_pool = None

    @staticmethod
    def _init_worker(engine_type: str):
        """Initialize the engine in a worker process.

        Required for spawn-based multiprocessing (Windows) where child processes
        start fresh without the parent's global state. Sets up the engine type
        and initializes the engine singleton so that `engine.get_instance()`
        works in worker processes.

        Parameters:
            engine_type: The engine type to initialize (e.g., "python", "spark").
        """
        import hopsworks_common.connection

        hopsworks_common.connection._hsfs_engine_type = engine_type
        engine.init(engine_type)

    @staticmethod
    def create_process_pool(n_processes: int = None):
        """Create a process pool for parallel execution of transformation functions.

        If a process pool already exists, it is shut down before creating a new one.
        Uses `fork` on Unix-based systems and `spawn` on Windows.

        Parameters:
            n_processes: Number of worker processes for executing transformation functions.
                If not provided, it is set to the maximum number of transformation functions that can run concurrently from the transfromation function execution DAG.
        """
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.shutdown_process_pool()
        mp_context = multiprocessing.get_context(
            "fork" if sys.platform != "win32" else "spawn"
        )
        TransformationFunctionEngine.__process_pool = ProcessPoolExecutor(
            max_workers=n_processes,
            mp_context=mp_context,
            initializer=TransformationFunctionEngine._init_worker,
            initargs=(engine.get_type(),),
        )

    @staticmethod
    def _validate_transformation_function_arguments(
        execution_graph: TransformationExecutionDAG,
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        request_parameters: dict[str, Any] | None = None,
    ) -> None:
        """Validate that all arguments required to execute the transformation functions are present in the passed data or request parameters.

        Parameters:
            execution_graph: The transformation DAG containing the transformation functions to validate.
            data: The dataframe or dictionary to validate the transformation functions against.
            request_parameters: Request parameters to validate the transformation functions against.

        Raises:
            exceptions.TransformationFunctionException: If the arguments required to execute the transformation functions are not present in the passed data or request parameters.
        """
        transformation_function_output_feature = set()
        if isinstance(request_parameters, list) and len(request_parameters) != len(
            data
        ):
            raise exceptions.TransformationFunctionException(
                "Request Parameters should be a Dictionary, None, empty or be a list having the same length as the number of rows in the data provided."
            )
        # Fast-path: dict/list skip the engine check entirely (avoids engine.get_instance() overhead)
        if isinstance(data, (dict, list)):
            is_dataframe = False
        else:
            is_dataframe = engine.get_instance().check_supported_dataframe(data)
            if not is_dataframe:
                raise exceptions.FeatureStoreException(
                    f"Dataframe type {type(data)} not supported in the engine."
                )

        data_features = set(data.columns) if is_dataframe else set(data.keys())
        for tf in execution_graph.nodes:
            missing_features = (
                set(tf.hopsworks_udf.transformation_features) - data_features
            )

            if request_parameters:
                missing_features = missing_features - set(request_parameters.keys())
                if tf.hopsworks_udf.feature_name_prefix:
                    missing_features = missing_features - {
                        tf.hopsworks_udf.feature_name_prefix + feature
                        for feature in request_parameters
                    }
            missing_features = missing_features - transformation_function_output_feature

            if missing_features:
                raise exceptions.TransformationFunctionException(
                    message=f"The following feature(s): `{', '.join(missing_features)}`, required for the transformation function '{tf.hopsworks_udf.function_name}' are not available.",
                    missing_features=missing_features,
                    transformation_function_name=tf.hopsworks_udf.function_name,
                    transformation_type=tf.transformation_type.value,
                )
            transformation_function_output_feature.update(
                tf.hopsworks_udf.output_column_names
            )

    @staticmethod
    def apply_transformation_functions(
        execution_graph: TransformationExecutionDAG,
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] = None,
        expected_features: set[str] = None,
        n_processes: int = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Apply the transformation functions from the DAG to the passed data.

        Uses a true DAG scheduler: each transformation starts as soon as its direct
        dependencies complete, rather than waiting for all transformations in a level.
        Transformations with no mutual dependencies run concurrently in a process pool.
        For the Spark engine, independent TFs are batched and pushed down to Spark.

        Parameters:
            execution_graph: The transformation DAG containing transformation functions with dependency tracking.
            data: The dataframe, dictionary, or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase.
                This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data.
                This is required to avoid dropping features with same names that are available from other feature groups in a feature view.
            n_processes: Number of worker processes for executing transformation functions.
                If not provided, it is set to the maximum number of transformation functions that can run concurrently from the transfromation function execution DAG.
                This parameter is only applicable when using the Python engine.
                In the Spark engine, the transformations are pushed down to Spark.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if execution_graph is None or not execution_graph.nodes or data is None:
            return data

        TransformationFunctionEngine._validate_transformation_function_arguments(
            execution_graph=execution_graph,
            data=data,
            request_parameters=request_parameters,
        )

        is_dataframe = not isinstance(data, (dict, list))

        # ============================================================
        # SPARK PATH — push entire DAG to the Spark engine
        # ============================================================
        if is_dataframe and engine.get_type() == "spark":
            return engine.get_instance()._apply_transformation_function(
                execution_graph=execution_graph,
                dataset=data,
                transformation_context=transformation_context,
                expected_features=expected_features,
                request_parameters=request_parameters,
            )

        # ============================================================
        # PYTHON PATH — pre-compute metadata
        # ============================================================
        dropped_features: set[str] = set()
        if is_dataframe:
            column_order = list(data.columns)

        for tf in execution_graph.nodes:
            udf = tf.hopsworks_udf
            udf.transformation_context = transformation_context
            if udf.dropped_features:
                dropped_features.update(
                    {f for f in udf.dropped_features if f not in expected_features}
                    if expected_features
                    else udf.dropped_features
                )
            if is_dataframe:
                for col in udf.output_column_names:
                    if col in column_order:
                        column_order.remove(col)
                    column_order.append(col)
        if request_parameters:
            data = TransformationFunctionEngine._update_request_parameter_data(
                data, request_parameters
            )

        # --- Dict/list: sequential, topo order, in-place update ---
        if isinstance(data, (dict, list)):
            if isinstance(data, list):
                transformed_data = [row.copy() for row in data]
                for tf in execution_graph.nodes:
                    for row in transformed_data:
                        result = TransformationFunctionEngine.execute_udf(
                            udf=tf.hopsworks_udf,
                            data=row,
                            online=online,
                            engine_type=engine.get_type(),
                        )
                        row.update(result)
                return [
                    {k: v for k, v in row.items() if k not in dropped_features}
                    for row in transformed_data
                ]
            transformed_data = data.copy()
            for tf in execution_graph.nodes:
                result = TransformationFunctionEngine.execute_udf(
                    udf=tf.hopsworks_udf,
                    data=transformed_data,
                    online=online,
                    engine_type=engine.get_type(),
                )
                transformed_data.update(result)
            return {
                k: v for k, v in transformed_data.items() if k not in dropped_features
            }

        # --- DataFrame: sequential (n_processes==1) or parallel DAG ---
        column_store = {}  # col_name -> Series (accumulated results from completed TFs)

        # Default to the max width of the DAG — no point spawning more workers
        # than transformations that can actually run concurrently.
        if n_processes is None:
            n_processes = execution_graph.max_parallelism

        if n_processes == 1:
            # Sequential topo-order iteration — deps satisfied by ordering
            for tf in execution_graph.nodes:
                needed = tf.hopsworks_udf.transformation_features
                col_data = {
                    c: column_store[c] if c in column_store else data[c] for c in needed
                }
                input_df = (
                    pl.DataFrame(col_data)
                    if HAS_POLARS and isinstance(data, pl.DataFrame)
                    else pd.DataFrame(col_data)
                )
                result = TransformationFunctionEngine.execute_udf(
                    udf=tf.hopsworks_udf,
                    data=input_df,
                    online=online,
                    engine_type=engine.get_type(),
                )
                for col in result.columns:
                    column_store[col] = result[col]
        else:
            # ---- TRUE DAG PARALLEL SCHEDULER ----
            # Uses wait(FIRST_COMPLETED): when ANY TF finishes, its dependents
            # are immediately submitted — no waiting for the whole "level".
            if not TransformationFunctionEngine.__process_pool or (
                n_processes
                and TransformationFunctionEngine.__process_pool._max_workers
                != n_processes
            ):
                TransformationFunctionEngine.create_process_pool(n_processes)
            pool = TransformationFunctionEngine.__process_pool

            remaining_deps = {
                id(tf): len(execution_graph.dependencies[id(tf)])
                for tf in execution_graph.nodes
            }
            ready_queue = deque(execution_graph.roots)
            future_to_tf = {}

            use_shm = HAS_PYARROW
            shm_ref = shm_name = shm_size = is_polars = None
            if use_shm:
                shm_ref, shm_size, is_polars = (
                    TransformationFunctionEngine._write_to_shared_memory(data)
                )
                shm_name = shm_ref.name

            try:
                while ready_queue or future_to_tf:
                    # Submit ALL currently-ready TFs to pool (they run in parallel)
                    while ready_queue:
                        tf = ready_queue.popleft()
                        needed = tf.hopsworks_udf.transformation_features
                        predecessor_cols = {
                            c: column_store[c] for c in needed if c in column_store
                        }
                        if use_shm:
                            future = pool.submit(
                                TransformationFunctionEngine.execute_udf,
                                udf=tf.hopsworks_udf,
                                shm_name=shm_name,
                                shm_size=shm_size,
                                is_polars=is_polars,
                                columns=needed,
                                predecessor_columns=predecessor_cols or None,
                                online=online,
                                engine_type=engine.get_type(),
                            )
                        else:
                            future = pool.submit(
                                TransformationFunctionEngine.execute_udf,
                                udf=tf.hopsworks_udf,
                                data=data,
                                online=online,
                                engine_type=engine.get_type(),
                            )
                        future_to_tf[future] = tf

                    # Wait for ANY one to finish (not all — this is the key)
                    done, _ = wait(future_to_tf.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        tf = future_to_tf.pop(future)
                        result = future.result()
                        for col in result.columns:
                            column_store[col] = result[col]
                        # Unblock dependents — if all deps done, add to ready queue
                        for dep_tf in execution_graph.dependents.get(id(tf), []):
                            remaining_deps[id(dep_tf)] -= 1
                            if remaining_deps[id(dep_tf)] == 0:
                                ready_queue.append(dep_tf)
            finally:
                if shm_ref is not None:
                    shm_ref.close()
                    shm_ref.unlink()

        # --- Merge column_store into original DataFrame ---
        if column_store:
            exec_engine = engine.get_instance()
            overwritten = set(column_store.keys()) & set(data.columns)
            base = exec_engine.drop_columns(data, overwritten) if overwritten else data
            result_df = (
                pl.DataFrame(column_store)
                if HAS_POLARS and isinstance(data, pl.DataFrame)
                else pd.DataFrame(column_store)
            )
            data = exec_engine.concat_dataframes([base, result_df])

        return data[[c for c in column_order if c not in dropped_features]]

    @staticmethod
    def _update_request_parameter_data(transformed_data, request_parameters):
        """Merge request parameters into the transformed data."""
        is_batch = isinstance(request_parameters, list)

        if isinstance(transformed_data, pd.DataFrame):
            if is_batch:
                return pd.concat(
                    [transformed_data, pd.DataFrame(request_parameters)], axis=1
                )
            return transformed_data.assign(**request_parameters)
        if HAS_POLARS and isinstance(transformed_data, pl.DataFrame):
            if is_batch:
                return pl.concat(
                    [transformed_data, pl.DataFrame(request_parameters)],
                    how="horizontal",
                )
            return transformed_data.with_columns(
                [pl.lit(v).alias(k) for k, v in request_parameters.items()]
            )
        if isinstance(transformed_data, dict):
            transformed_data.update(request_parameters)
            return transformed_data
        # list of dicts
        if is_batch:
            for row, rq in zip(transformed_data, request_parameters):
                row.update(rq)
        else:
            for row in transformed_data:
                row.update(request_parameters)
        return transformed_data

    @staticmethod
    def _write_to_shared_memory(
        dataframe: pd.DataFrame,
    ) -> tuple[shared_memory.SharedMemory, int, bool]:
        """Serialize a DataFrame to Arrow IPC format and write it to shared memory.

        The returned SharedMemory object is intentionally left open so that the
        mapping stays alive on Windows (where the mapping is destroyed when the
        last handle is closed).
        The caller must call `shm.close()` and `shm.unlink()` after all workers
        are done.

        Parameters:
            dataframe: A pandas or polars DataFrame to serialize.

        Returns:
            A tuple of (shared_memory_object, buffer_size, is_polars).
            Use `shm.name` to get the name for passing to worker processes.
        """
        is_polars = HAS_POLARS and isinstance(dataframe, pl.DataFrame)
        if is_polars:
            table = dataframe.to_arrow()
        else:
            table = pa.Table.from_pandas(dataframe, preserve_index=False)

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        buf = sink.getvalue()  # pa.Buffer — no Python bytes copy

        shm = shared_memory.SharedMemory(create=True, size=buf.size)
        shm.buf[: buf.size] = memoryview(buf).cast("B")  # single C-level memcpy
        # Do NOT close here — on Windows the mapping is destroyed when the
        # last handle is closed.  The caller must keep this object alive and
        # call close()/unlink() after all workers are done.
        return shm, buf.size, is_polars

    @staticmethod
    def _read_from_shared_memory(
        shm_name: str,
        shm_size: int,
        as_polars: bool = False,
    ) -> pd.DataFrame:
        """Read a DataFrame from an Arrow IPC stream stored in shared memory.

        The shared memory block is opened read-only (create=False) and closed
        after reading. The caller (master process) is responsible for unlinking.

        Parameters:
            shm_name: Name of the shared memory block.
            shm_size: Size of the serialized Arrow IPC data in bytes.
            as_polars: If True, return a polars DataFrame; otherwise pandas.

        Returns:
            The deserialized DataFrame (pandas or polars).
        """
        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        try:
            if as_polars:
                # Polars retains zero-copy references to Arrow memory, so we
                # must copy the bytes out of shared memory before closing it.
                reader = ipc.open_stream(bytes(shm.buf[:shm_size]))
                result = pl.from_arrow(reader.read_all())
            else:
                # Wrap shared memory as an Arrow buffer — zero-copy read.
                # to_pandas() copies into numpy arrays, releasing Arrow refs.
                buf = pa.py_buffer(shm.buf[:shm_size])
                table = ipc.open_stream(buf).read_all()
                result = table.to_pandas()
                del table, buf
        finally:
            shm.close()
        return result

    @staticmethod
    def execute_udf(
        udf: HopsworksUdf,
        data: spark_sql.DataFrame
        | pl.DataFrame
        | pd.DataFrame
        | dict[str, Any]
        | list[dict[str, Any]]
        | None = None,
        engine_type: str | None = None,
        online: bool = False,
        shm_name: str | None = None,
        shm_size: int | None = None,
        is_polars: bool = False,
        columns: list[str] | None = None,
        predecessor_columns: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Function to execute the UDF used to defined a transformation function on passed data.

        The functions pushes the execution of Pandas and Python Dataframes to the Python Engine and handles the execution dictionaries.

        When `shm_name` is provided, the DataFrame is read from Arrow shared memory
        instead of being deserialized via pickle. Only the columns required by the UDF
        are selected, reducing memory usage in each worker.

        Parameters:
            udf: The transformation function to execute.
            data: The dataframe, dictionary, or list of dictionaries to execute the transformation function on.
                Can be None when shared memory parameters are provided instead.
            engine_type: The engine type to use for execution.
                When set to `"spark"`, offline dictionary execution is not supported.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            shm_name: Name of the shared memory block containing the Arrow IPC data.
            shm_size: Size in bytes of the Arrow IPC data in shared memory.
            is_polars: Whether to deserialize the Arrow data as a polars DataFrame.
            columns: Column names to select from the shared memory DataFrame.
            predecessor_columns: Columns from predecessor TFs to override base data columns.
                Used in DAG parallel execution where predecessor results must be
                passed to dependent TFs.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if shm_name is not None:
            data = TransformationFunctionEngine._read_from_shared_memory(
                shm_name, shm_size, is_polars
            )
            if columns:
                if predecessor_columns:
                    # Build input: base columns overridden by predecessor results
                    col_data = {}
                    for col in columns:
                        col_data[col] = (
                            predecessor_columns[col]
                            if col in predecessor_columns
                            else data[col]
                        )
                    data = (
                        pl.DataFrame(col_data) if is_polars else pd.DataFrame(col_data)
                    )
                else:
                    data = data[columns]

        # Check dict/list first — these are the dominant types on the online
        # serving hot path and avoid the multiple isinstance() checks inside
        # check_supported_dataframe() that all fail for non-DataFrame types.
        if isinstance(data, dict):
            return TransformationFunctionEngine.apply_udf_on_dict(
                udf=udf, data=data, online=online, engine_type=engine_type
            )
        if isinstance(data, list):
            return [
                TransformationFunctionEngine.apply_udf_on_dict(
                    udf=udf, data=row, online=online, engine_type=engine_type
                )
                for row in data
            ]
        execution_engine = engine.get_instance()
        if execution_engine.check_supported_dataframe(data):
            return execution_engine.apply_udf_on_dataframe(
                udf=udf, dataframe=data, online=online, engine_type=engine_type
            )
        raise exceptions.FeatureStoreException(
            f"Dataframe type {type(data)} not supported in the engine."
        )

    @staticmethod
    def apply_udf_on_dict(
        udf: HopsworksUdf,
        data: dict[str, Any],
        online: bool | None = True,
        engine_type: str | None = None,
    ) -> dict[str, Any]:
        """Apply the UDF of a transformation function on a single dictionary record.

        This function is not pushed to the Python Engine since it needs to be executable in both the Python and Spark kernels.

        Parameters:
            udf: The transformation function to execute.
            data: The dictionary to execute the transformation function on.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            engine_type: The engine type to use for execution.
                When set to `"spark"`, offline dictionary execution raises an error because Spark requires DataFrames.

        Returns:
            A dictionary containing only the transformed output columns.
        """
        if not online and engine_type == "spark":
            raise exceptions.FeatureStoreException(
                "Cannot apply transformation functions on a dictionary in offline mode when the engine is spark. Please use the python engine or use the online mode."
            )

        is_pandas_mode = (
            udf.execution_mode.get_current_execution_mode(online=online)
            == UDFExecutionMode.PANDAS
        )

        # Pre-compute prefix and feature list once to avoid repeated property
        # access and string concatenation inside the loop.
        prefix = udf.feature_name_prefix

        if is_pandas_mode:
            features = []
            for feat in udf.unprefixed_transformation_features:
                feature_name = prefix + feat if prefix else feat
                val = data[feature_name] if feature_name in data else data[feat]
                features.append(pd.Series([val], name=feat))
        else:
            features = []
            for feat in udf.unprefixed_transformation_features:
                feature_name = prefix + feat if prefix else feat
                features.append(
                    data[feature_name] if feature_name in data else data[feat]
                )

        transformed_result = udf.get_udf(online=online, engine_type=engine_type)(
            *features
        )

        transformed_dict = {}

        if is_pandas_mode:
            # Pandas UDF return can return a pandas series or a pandas dataframe, so we need to cast it back to a dictionary.
            if isinstance(transformed_result, pd.Series):
                transformed_dict[transformed_result.name] = transformed_result.values[0]
            else:
                for col in transformed_result:
                    transformed_dict[col] = transformed_result[col].values[0]
        else:
            # Python UDF return can return a tuple or a list, so we need to cast it back to a dictionary.
            if isinstance(transformed_result, (tuple, list)):
                for index, result in enumerate(transformed_result):
                    transformed_dict[udf.output_column_names[index]] = result
            else:
                transformed_dict[udf.output_column_names[0]] = transformed_result

        return transformed_dict

    def delete(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> None:
        """Delete a transformation function from the feature store.

        Parameters:
            transformation_function_instance: The transformation function to be removed from the feature store.
        """
        self._transformation_function_api.delete(transformation_function_instance)

    @staticmethod
    def compute_transformation_fn_statistics(
        training_dataset_obj: training_dataset.TrainingDataset,
        statistics_features: list[str],
        label_encoder_features: list[str],
        feature_dataframe: pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        feature_view_obj: feature_view.FeatureView,
    ) -> statistics.Statistics:
        """Compute the statistics required for a training dataset object.

        Parameters:
            training_dataset_obj : The training dataset for which the statistics is to be computed.
            statistics_features : The list of features for which the statistics should be computed.
            label_encoder_features : Features used for label encoding.
            feature_dataframe : The dataframe that contains the data for which the statistics must be computed.
            feature_view_obj : The feature view in which the training data is being created.

        Returns:
            The statistics object that contains the statistics for each features.
        """
        return training_dataset_obj._statistics_engine.compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=statistics_features,
            label_encoder_features=label_encoder_features,  # label encoded features only
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def get_ready_to_use_transformation_fns(
        feature_view: feature_view.FeatureView,
        training_dataset_version: int | None = None,
    ) -> list[transformation_function.TransformationFunction]:
        """Function that updates statistics required for all transformation functions in the feature view based on training dataset version.

        Parameters:
            feature_view: The feature view in which the training data is being created.
            training_dataset_version: The training version used to update the statistics used in the transformation functions.

        Returns:
            List of transformation functions.
        """
        # check if transformation functions require statistics
        is_stat_required = any(
            tf.hopsworks_udf.statistics_required
            for tf in feature_view.transformation_functions
        )
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any transformation functions that require statistics get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with before_transformation=true
            if training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` "
                    "or `feature_view.init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
                )
            td_tffn_stats = feature_view._statistics_engine.get(
                feature_view,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
                "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
            )

        if is_stat_required:
            for transformation_function in feature_view.transformation_functions:
                transformation_function.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
        return feature_view.transformation_functions

    @staticmethod
    def compute_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        dataset: dict[
            str, pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame")
        ]
        | pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
    ) -> None:
        """Function that computes and sets the statistics required for the UDF used for transformation.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        Parameters:
            training_dataset : The training dataset for which the statistics is to be computed.
            feature_view_obj : The feature view in which the training data is being created.
            dataset : A dataframe that contains the training data or a dictionary that contains both the training and test data.
        """
        statistics_features: set[str] = set()
        label_encoder_features: set[str] = set()

        # Finding the features for which statistics is required
        for tf in feature_view_obj.transformation_functions:
            statistics_features.update(tf.hopsworks_udf.statistics_features)
            if (
                tf.hopsworks_udf.function_name == "label_encoder"
                or tf.hopsworks_udf.function_name == "one_hot_encoder"
            ):
                label_encoder_features.update(tf.hopsworks_udf.statistics_features)
        if statistics_features:
            # compute statistics on training data
            if training_dataset.splits:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset.get(training_dataset.train_split),
                        feature_view_obj,
                    )
                )
            else:
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset,
                        feature_view_obj,
                    )
                )

            # Set statistics computed in the hopsworks UDF
            for tf in feature_view_obj.transformation_functions:
                tf.transformation_statistics = stats.feature_descriptive_statistics

    @staticmethod
    def build_transformation_function_execution_graph(
        transformation_functions,
    ) -> TransformationExecutionDAG:
        """Build a DAG (Directed Acyclic Graph) to determine the execution order of transformation functions.

        Analyzes the dependencies between transformation functions by inspecting their input and output features.
        The resulting DAG contains topologically sorted nodes with per-node dependency and dependent tracking,
        enabling true parallel execution where each transformation starts as soon as its direct dependencies complete.

        Parameters:
            transformation_functions: Flat list of transformation functions to organize into a DAG.

        Returns:
            A `TransformationExecutionDAG` containing topo-sorted nodes, dependency/dependent maps, and root nodes.

        Raises:
            TransformationFunctionException: If a cyclic dependency is detected among the transformation functions.
        """
        funcs = {
            output_column_name: tf
            for tf in transformation_functions
            for output_column_name in tf.hopsworks_udf.output_column_names
        }
        # Build adjacency: edges[u] = set of v where u -> v (u must complete before v)
        all_nodes = set(funcs.keys())
        edges: dict[str, set[str]] = {n: set() for n in all_nodes}
        in_degree: dict[str, int] = dict.fromkeys(all_nodes, 0)

        for name, tf in funcs.items():
            transformation_features = tf.hopsworks_udf.transformation_features
            # Skip self-overwrite edges (would create false cycles)
            if not any(
                f == tf.hopsworks_udf.function_name for f in transformation_features
            ):
                for feat in transformation_features:
                    if feat in funcs:
                        edges[feat].add(name)
                        in_degree[name] += 1

        # Kahn's algorithm: topological sort + cycle detection in one pass
        queue = deque(n for n in all_nodes if in_degree[n] == 0)
        topo_node_names: list[str] = []
        while queue:
            node = queue.popleft()
            topo_node_names.append(node)
            for neighbor in edges[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(topo_node_names) != len(all_nodes):
            raise exceptions.TransformationFunctionException(
                "Cyclic dependency detected in transformation functions."
            )

        # Deduplicate TFs (multiple output columns map to same TF), preserving topological order
        seen: set[int] = set()
        topo_tfs: list = []
        for node_name in topo_node_names:
            tf = funcs[node_name]
            if id(tf) not in seen:
                seen.add(id(tf))
                topo_tfs.append(tf)

        # Build per-TF dependency/dependent maps
        id_to_tf: dict[int, Any] = {id(tf): tf for tf in topo_tfs}
        dep_ids: dict[int, set[int]] = {id(tf): set() for tf in topo_tfs}
        dept_ids: dict[int, set[int]] = {id(tf): set() for tf in topo_tfs}

        for u_name, neighbors in edges.items():
            for v_name in neighbors:
                tf_u, tf_v = funcs[u_name], funcs[v_name]
                dep_ids[id(tf_v)].add(id(tf_u))
                dept_ids[id(tf_u)].add(id(tf_v))

        dependencies: dict[int, list] = {
            tid: [id_to_tf[d] for d in deps] for tid, deps in dep_ids.items()
        }
        dependents: dict[int, list] = {
            tid: [id_to_tf[d] for d in deps] for tid, deps in dept_ids.items()
        }
        roots = [tf for tf in topo_tfs if not dependencies[id(tf)]]

        return TransformationExecutionDAG(
            nodes=topo_tfs,
            dependencies=dependencies,
            dependents=dependents,
            roots=roots,
        )

    @staticmethod
    def get_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        training_dataset_version: int = None,
    ) -> None:
        """Function that gets the transformation statistics computed while creating the training dataset from the backend and assigns it to the hopsworks UDF object.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        Parameters:
            training_dataset: The training dataset for which the statistics is to be computed.
            feature_view_obj: The feature view in which the training data is being created.
            training_dataset_version: The version of the training dataset for which the statistics is to be retrieved.

        Raises:
            ValueError: If the statistics are not present in the backend.
        """
        is_stat_required = any(
            tf.hopsworks_udf.statistics_required
            for tf in feature_view_obj.transformation_functions
        )

        if is_stat_required:
            td_tffn_stats = training_dataset._statistics_engine.get(
                feature_view_obj,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

            if td_tffn_stats is None:
                raise ValueError(
                    "No statistics available for initializing transformation functions."
                )

            for tf in feature_view_obj.transformation_functions:
                tf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )

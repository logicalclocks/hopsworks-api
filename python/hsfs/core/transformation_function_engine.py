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
import errno
import logging
import multiprocessing
import os
import sys
import warnings
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    wait,
)
from concurrent.futures.process import BrokenProcessPool
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
from hsfs.core.transformation_execution_dag import TransformationExecutionDAG
from hsfs.hopsworks_udf import HopsworksUdf, UDFExecutionMode


_logger = logging.getLogger(__name__)

_IS_TF_WORKER: bool = False


def _warmup_barrier_wait(barrier, timeout):
    """Worker-side warmup task: rendezvous on the barrier, then return.

    Module-level so it pickles cleanly under spawn / forkserver start methods.
    Submitting `n_processes` instances of this task with a `Barrier(n_processes)`
    forces ProcessPoolExecutor to spawn every configured worker before any task
    can complete, turning a lazily-spawned pool into a fully-warm one.

    The timeout bounds the rendezvous: if the pool cannot scale to the full
    worker count (resource limits, failed spawns) the barrier breaks, every
    parked worker unblocks with `BrokenBarrierError`, and the parent surfaces
    the failure instead of hanging forever.
    """
    barrier.wait(timeout)


if HAS_POLARS:
    import polars as pl

if HAS_PYARROW:
    import pyarrow as pa
    import pyarrow.ipc as ipc


if TYPE_CHECKING:
    from collections.abc import Callable

    import pyspark.sql as spark_sql


class TransformationFunctionEngine:
    BUILTIN_FN_NAMES = [
        "min_max_scaler",
        "standard_scaler",
        "robust_scaler",
        "label_encoder",
        "one_hot_encoder",
        "log_transform",
        "quantile_transformer",
        "rank_normalizer",
        "winsorize",
        "equal_width_binner",
        "equal_frequency_binner",
        "quantile_binner",
        "top_k_categorical_binner",
        "impute_mean",
        "impute_median",
        "impute_constant",
        "impute_mode",
        "impute_category",
    ]
    AMBIGUOUS_FEATURE_ERROR = (
        "Provided feature '{}' in transformation functions is ambiguous and exists in more than one feature groups."
        "You can provide the feature with the prefix that was specified in the join."
    )
    FEATURE_NOT_EXIST_ERROR = "Provided feature '{}' in transformation functions do not exist in any of the feature groups."

    # Class-level singleton process pool. Shared across instances so a single
    # pool is reused for every transformation-function invocation in the
    # current Python process.
    #
    # Thread-safety note: the Hopsworks Python client is single-threaded by
    # contract. If a user invokes the client from multiple threads behavior
    # is undefined. The guarantee Hopsworks itself must hold is that no
    # internal Hopsworks code path concurrently invokes
    # _apply_transformation_functions on the same process; this code relies on
    # that. The internal async / threaded subsystems
    # (hopsworks_common.util.AsyncTaskThread, hsfs.feature_logger_async,
    # online_store_sql_engine, dataset_api uploads) operate on read/log
    # paths and never call _apply_transformation_functions concurrently, so
    # the pool create / swap / shutdown sequence below is lock-free.
    __process_pool: ProcessPoolExecutor | None = None
    __process_pool_n_processes: int | None = None

    _VALID_START_METHODS: frozenset[str] = frozenset({"fork", "forkserver", "spawn"})
    _start_method_warned: bool = False

    def __init__(self, feature_store_id: int):
        self._feature_store_id = feature_store_id
        self._transformation_function_api: transformation_function_api.TransformationFunctionApi = transformation_function_api.TransformationFunctionApi(
            feature_store_id
        )

    def _save(
        self, transformation_fn_instance: transformation_function.TransformationFunction
    ):
        """Save a transformation function into the feature store.

        Parameters:
            transformation_fn_instance: The transformation function to be saved into the feature store.
        """
        self._transformation_function_api._register_transformation_fn(
            transformation_fn_instance
        )

    def _get_transformation_fn(
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
        return self._transformation_function_api._get_transformation_fn(name, version)

    def _get_transformation_fns(
        self,
    ) -> list[transformation_function.TransformationFunction]:
        """Get all the transformation functions in the feature store.

        Returns:
            A list of transformation functions.
        """
        transformation_fn_instances = (
            self._transformation_function_api._get_transformation_fn(
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
    def _shutdown_process_pool(
        *, wait: bool = True, cancel_futures: bool = False
    ) -> None:
        """Shut down the process pool (if any) and clear its state.

        Safe to call when no pool exists. The shutdown is guarded so a failure to
        stop the pool still clears the singleton, and ``__process_pool`` /
        ``__process_pool_n_processes`` are reset together so the next call lazily
        builds a fresh pool.

        The defaults block until running tasks finish (the normal and ``atexit``
        path). The failure path passes ``wait=False, cancel_futures=True`` to drop
        a broken pool without blocking and to cancel queued tasks, so workers do
        not linger until garbage collection on a long-running service.

        Parameters:
            wait: Whether to block until running tasks finish.
            cancel_futures: Whether to cancel queued, not-yet-started tasks.
        """
        pool = TransformationFunctionEngine.__process_pool
        if pool is not None:
            try:
                pool.shutdown(wait=wait, cancel_futures=cancel_futures)
            except Exception as exc:
                _logger.warning("Pool shutdown raised: %s", exc)
        TransformationFunctionEngine.__process_pool = None
        TransformationFunctionEngine.__process_pool_n_processes = None

    @staticmethod
    def _init_worker(engine_type: str):
        """Initialize the engine singleton in a worker process (needed for spawn)."""
        global _IS_TF_WORKER
        _IS_TF_WORKER = True
        import hopsworks_common.connection

        hopsworks_common.connection._hsfs_engine_type = engine_type
        engine._init(engine_type)

    @staticmethod
    def _warmup_online_workers(n_processes: int, timeout: float = 60.0) -> None:
        """Pre-spawn the transformation worker pool.

        Triggered from `FeatureView.init_serving` / `init_batch_scoring` when
        parallel execution is requested, so the first `get_feature_vector(s)` or
        batch call does not pay process-spawn and engine-init latency. A pool
        already warmed with the same `n_processes` is reused; a different
        `n_processes` replaces it.

        Each worker initializes its own Hopsworks engine singleton on startup, so
        for online UDFs that touch the online store the resident-memory and
        connection cost scales with `n_processes`.

        Parameters:
            n_processes: Number of worker processes to pre-spawn.
            timeout: Seconds to wait for all workers to spawn and rendezvous.

        Raises:
            FeatureStoreException: If `n_processes < 1`, or if the pool fails to warm `n_processes` workers within `timeout` seconds.
        """
        if n_processes < 1:
            raise exceptions.FeatureStoreException(
                "Warming the transformation worker pool requires n_processes >= 1."
            )
        TransformationFunctionEngine._ensure_pool(n_processes)
        TransformationFunctionEngine._force_full_spawn(n_processes, timeout)

    @staticmethod
    def _force_full_spawn(n_processes: int, timeout: float) -> None:
        """Force the pool to spawn all `n_processes` workers before returning.

        ProcessPoolExecutor spawns workers lazily on first submit, so submitting
        `n_processes` tasks that all rendezvous on a shared Barrier forces every
        worker to start: no task can return until all of them reach the barrier.
        The barrier and the result collection are bounded by `timeout`, so a pool
        that cannot reach `n_processes` workers breaks the barrier in every parked
        worker instead of deadlocking the caller.

        Raises:
            FeatureStoreException: If the pool fails to warm `n_processes` workers within `timeout` seconds.
        """
        pool = TransformationFunctionEngine.__process_pool
        manager = multiprocessing.Manager()
        try:
            barrier = manager.Barrier(n_processes)
            futures = [
                pool.submit(_warmup_barrier_wait, barrier, timeout)
                for _ in range(n_processes)
            ]
            try:
                for future in futures:
                    future.result(timeout=timeout)
            except Exception as e:
                TransformationFunctionEngine._shutdown_process_pool(
                    wait=False, cancel_futures=True
                )
                raise exceptions.FeatureStoreException(
                    f"Worker pool failed to warm up {n_processes} workers within "
                    f"{timeout}s. The pool has been discarded; check process and "
                    "memory limits, or lower n_processes."
                ) from e
        finally:
            manager.shutdown()

    @staticmethod
    def _resolve_mp_start_method() -> str:
        """Resolve the multiprocessing start method for the TF worker pool.

        `fork` is the fastest start method and enables the shared-memory path,
        but forking a process that already holds live native threads (numpy,
        OpenMP, Arrow) can deadlock the child. It is only safe enough to default
        to on Linux; macOS and Windows default to `spawn`.

        The `HSFS_TF_POOL_START_METHOD` environment variable overrides the
        default with `fork`, `forkserver`, or `spawn` (except on Windows, where
        `fork` is unavailable).
        """
        if sys.platform == "win32":
            return "spawn"
        default = "fork" if sys.platform.startswith("linux") else "spawn"
        override = os.environ.get("HSFS_TF_POOL_START_METHOD")
        if override is None:
            return default
        if override in TransformationFunctionEngine._VALID_START_METHODS:
            return override
        # Warn once per session. _resolve_mp_start_method is called on every
        # pool create / swap, so without the latch a persistent bad env value
        # would flood the log of a long-running process.
        if not TransformationFunctionEngine._start_method_warned:
            _logger.warning(
                "HSFS_TF_POOL_START_METHOD=%r is not one of %s; defaulting to %r.",
                override,
                sorted(TransformationFunctionEngine._VALID_START_METHODS),
                default,
            )
            TransformationFunctionEngine._start_method_warned = True
        return default

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
        # Fast-path: dict/list skip the engine check entirely (avoids engine._get_instance() overhead)
        if isinstance(data, (dict, list)):
            is_dataframe = False
        else:
            is_dataframe = engine._get_instance()._check_supported_dataframe(data)
            if not is_dataframe:
                raise exceptions.FeatureStoreException(
                    f"Dataframe type {type(data)} not supported in the engine."
                )

        # A list of per-row request parameters is only valid against row-shaped
        # data (a DataFrame or list of dicts), and must match its row count. A
        # single dict is one row, so `len(data)` (its key count) is not the row
        # count and must not be used here.
        if isinstance(request_parameters, list):
            n_rows = 1 if isinstance(data, dict) else len(data)
            if len(request_parameters) != n_rows:
                raise exceptions.TransformationFunctionException(
                    "Request Parameters should be a Dictionary, None, empty or be a list having the same length as the number of rows in the data provided."
                )

        # Compute available keys per "row" so a feature counts as available
        # for validation only when it is reachable on every row at execution.
        # Per-row execution merges request_parameters into the data row
        # before invoking the UDF, so the effective per-row available keys
        # are (row data keys) UNION (row request_parameter keys). A feature
        # missing on some rows but present via request_parameters on others
        # must still pass validation as long as every row has at least one
        # source for it.
        if is_dataframe:
            data_keys_per_row: list[set[str]] = [set(data.columns)]
        elif isinstance(data, list):
            data_keys_per_row = [set(row.keys()) for row in data] or [set()]
        else:
            data_keys_per_row = [set(data.keys())]

        if request_parameters:
            if isinstance(request_parameters, list):
                rp_keys_per_row: list[set[str]] = [
                    set(rp.keys()) for rp in request_parameters
                ] or [set()]
            else:
                rp_keys_per_row = [set(request_parameters.keys())]
        else:
            rp_keys_per_row = [set()]

        # For DataFrame input, _update_request_parameter_data concatenates
        # list-shaped request_parameters into a DataFrame with the union of
        # keys (missing values fill as NaN), so on the DataFrame path the
        # "available" set is the union of every row's request_parameter
        # keys, not their intersection.
        if is_dataframe and len(rp_keys_per_row) > 1:
            rp_keys_per_row = [set().union(*rp_keys_per_row)]

        # Broadcast singletons to match list lengths, then per-row union and
        # cross-row intersection.
        n_rows = max(len(data_keys_per_row), len(rp_keys_per_row))
        if len(data_keys_per_row) == 1:
            data_keys_per_row = data_keys_per_row * n_rows
        if len(rp_keys_per_row) == 1:
            rp_keys_per_row = rp_keys_per_row * n_rows
        per_row_available = [
            d | r for d, r in zip(data_keys_per_row, rp_keys_per_row, strict=False)
        ]
        available_features: set[str] = (
            per_row_available[0].copy() if per_row_available else set()
        )
        for row_keys in per_row_available[1:]:
            available_features.intersection_update(row_keys)

        # Also gather the cross-row available request-parameter keys for the
        # prefix-fallback step below. Same intersection rule: a prefixed
        # lookup only counts as resolvable when every row supplies the key.
        rp_available: set[str] = rp_keys_per_row[0].copy() if rp_keys_per_row else set()
        for rp in rp_keys_per_row[1:]:
            rp_available.intersection_update(rp)

        for tf in execution_graph.nodes:
            missing_features = (
                set(tf.hopsworks_udf.transformation_features) - available_features
            )
            if tf.hopsworks_udf.feature_name_prefix and rp_available:
                missing_features = missing_features - {
                    tf.hopsworks_udf.feature_name_prefix + feature
                    for feature in rp_available
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
    def _resolve_n_processes(
        execution_graph: TransformationExecutionDAG, n_processes: int | None
    ) -> int:
        """Resolve the worker count for applying a DAG.

        Execution is sequential unless the caller asks for workers. A request
        above the DAG's maximum parallelism cannot be used (no more
        transformations can ever run concurrently), so it is capped and a
        warning tells the caller the effective ceiling.

        Parameters:
            execution_graph: The DAG the workers would execute.
            n_processes: The caller's requested worker count, if any.

        Returns:
            The worker count to use.
        """
        if n_processes is None:
            return 1
        if n_processes > execution_graph.max_parallelism:
            warnings.warn(
                f"n_processes={n_processes} exceeds the maximum parallelism of "
                f"the transformation DAG: at most {execution_graph.max_parallelism} "
                f"transformation(s) can run concurrently. Using "
                f"n_processes={execution_graph.max_parallelism}.",
                stacklevel=2,
            )
            return execution_graph.max_parallelism
        return n_processes

    @staticmethod
    def _apply_transformation_functions(
        execution_graph: TransformationExecutionDAG | None = None,
        data: spark_sql.DataFrame
        | pl.DataFrame
        | pd.DataFrame
        | dict[str, Any]
        | list[dict[str, Any]]
        | None = None,
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] | None = None,
        request_parameters: dict[str, Any] | None = None,
        expected_features: set[str] | None = None,
        n_processes: int | None = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Apply the transformation functions in the DAG to the passed data.

        Each transformation starts as soon as its direct dependencies complete,
        so independent transformations run concurrently in a process pool while a
        chained sequence runs in order. For the Spark engine the whole DAG is
        pushed down to Spark.

        Parameters:
            execution_graph: The transformation DAG to apply.
            data: The dataframe, dictionary, or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase.
                This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data.
                This is required to avoid dropping features with same names that are available from other feature groups in a feature view.
            n_processes: Number of worker processes for applying transformation functions in parallel.
                Defaults to `1` (sequential execution); a value above the DAG's maximum parallelism is capped, with a warning.
                Ignored by the Spark engine.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if _IS_TF_WORKER:
            n_processes = 1

        if execution_graph is None or not execution_graph.nodes or data is None:
            return data

        TransformationFunctionEngine._validate_transformation_function_arguments(
            execution_graph=execution_graph,
            data=data,
            request_parameters=request_parameters,
        )

        is_dataframe = not isinstance(data, (dict, list))

        # Spark path: push the entire DAG down to the Spark engine.
        if is_dataframe and engine._get_type() == "spark":
            return engine._get_instance()._apply_transformation_function(
                execution_graph=execution_graph,
                dataset=data,
                transformation_context=transformation_context,
                expected_features=expected_features,
                request_parameters=request_parameters,
            )

        n_processes = TransformationFunctionEngine._resolve_n_processes(
            execution_graph, n_processes
        )

        # Python path. Set the transformation context on every UDF, inject any
        # request parameters, then delegate to the handler for the input shape.
        for tf in execution_graph.nodes:
            tf.hopsworks_udf.transformation_context = transformation_context
        if request_parameters:
            data = TransformationFunctionEngine._update_request_parameter_data(
                data, request_parameters
            )

        if is_dataframe:
            return TransformationFunctionEngine._apply_to_dataframe(
                execution_graph, data, online, n_processes, expected_features
            )
        return TransformationFunctionEngine._apply_to_dict_list(
            execution_graph, data, online, n_processes, expected_features
        )

    @staticmethod
    def _transformed_column_layout(
        execution_graph: TransformationExecutionDAG,
        data_columns: list[str],
        expected_features: set[str] | None,
    ) -> tuple[set[str], list[str]]:
        """Compute the dropped features and the final column order for a transform.

        Drops are applied as a single projection after every transformation has
        run, not interleaved, so this layout describes the result: the original
        columns that survive, followed by the transformation outputs in
        topological order.

        Parameters:
            execution_graph: The transformation DAG.
            data_columns: Columns present in the input (empty when only the dropped set is needed).
            expected_features: Features kept even when a transformation drops them, so a feature shared across feature groups is not removed.

        Returns:
            The set of dropped feature names and the ordered list of resulting columns.
        """
        dropped_features: set[str] = set()
        tf_output_cols: list[str] = []
        tf_output_set: set[str] = set()
        for tf in execution_graph.nodes:
            udf = tf.hopsworks_udf
            if udf.dropped_features:
                dropped_features.update(
                    {f for f in udf.dropped_features if f not in expected_features}
                    if expected_features is not None
                    else udf.dropped_features
                )
            for col in udf.output_column_names:
                tf_output_set.add(col)
                tf_output_cols.append(col)
        column_order = [
            c for c in data_columns if c not in tf_output_set
        ] + tf_output_cols
        return dropped_features, column_order

    @staticmethod
    def _apply_to_dict_list(
        execution_graph: TransformationExecutionDAG,
        data: dict[str, Any] | list[dict[str, Any]],
        online: bool,
        n_processes: int | None,
        expected_features: set[str] | None,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        """Apply the transformations to a single dict or a list of dicts.

        Independent transformations run concurrently in a worker pool when the
        caller opts in (`n_processes > 1`) and the chain has independent branches;
        otherwise they run sequentially in-process. A linear chain has nothing to
        overlap, so the pool would only add per-function IPC and run slower than a
        single process. The default is sequential because the pool overhead rarely
        pays off for per-row work.
        """
        is_list = isinstance(data, list)
        rows = [row.copy() for row in data] if is_list else [data.copy()]
        engine_type = engine._get_type()
        dropped_features, _ = TransformationFunctionEngine._transformed_column_layout(
            execution_graph, [], expected_features
        )

        def task_inputs(tf):
            # Ship only this function's inputs for every row; a consumed
            # intermediate is present because its producer's outputs were merged
            # before this function ran. _execute_udf maps over the list.
            features = tf.hopsworks_udf.transformation_features
            return [{f: row.get(f) for f in features} for row in rows]

        def merge(tf, result):
            for row, output in zip(rows, result, strict=False):
                row.update(output)

        if n_processes > 1:
            TransformationFunctionEngine._ensure_pool(n_processes)
            pool = TransformationFunctionEngine.__process_pool
            TransformationFunctionEngine._schedule_dag(
                execution_graph,
                submit=lambda tf: pool.submit(
                    TransformationFunctionEngine._execute_udf,
                    udf=tf.hopsworks_udf,
                    data=task_inputs(tf),
                    online=online,
                    engine_type=engine_type,
                ),
                collect=merge,
            )
        else:
            for tf in execution_graph.nodes:
                merge(
                    tf,
                    TransformationFunctionEngine._execute_udf(
                        udf=tf.hopsworks_udf,
                        data=task_inputs(tf),
                        online=online,
                        engine_type=engine_type,
                    ),
                )

        cleaned = [
            {k: v for k, v in row.items() if k not in dropped_features} for row in rows
        ]
        return cleaned if is_list else cleaned[0]

    @staticmethod
    def _apply_to_dataframe(
        execution_graph: TransformationExecutionDAG,
        data: pd.DataFrame | pl.DataFrame,
        online: bool,
        n_processes: int | None,
        expected_features: set[str] | None,
    ) -> pd.DataFrame | pl.DataFrame:
        """Apply the transformations to a pandas or polars DataFrame.

        Each transformation is vectorized over the whole column, so independent
        transformations are the unit of parallelism: with `n_processes > 1` they
        run concurrently in a worker pool, otherwise sequentially in-process.
        """
        engine_type = engine._get_type()
        is_polars = HAS_POLARS and isinstance(data, pl.DataFrame)
        dropped_features, column_order = (
            TransformationFunctionEngine._transformed_column_layout(
                execution_graph, list(data.columns), expected_features
            )
        )
        # Outputs of completed transformations, keyed by column name.
        column_store: dict[str, Any] = {}

        def merge(tf, result):
            for col in result.columns:
                column_store[col] = result[col]

        if n_processes == 1:
            for tf in execution_graph.nodes:
                needed = tf.hopsworks_udf.transformation_features
                col_data = {
                    c: column_store[c] if c in column_store else data[c] for c in needed
                }
                input_df = (
                    pl.DataFrame(col_data) if is_polars else pd.DataFrame(col_data)
                )
                merge(
                    tf,
                    TransformationFunctionEngine._execute_udf(
                        udf=tf.hopsworks_udf,
                        data=input_df,
                        online=online,
                        engine_type=engine_type,
                    ),
                )
        else:
            TransformationFunctionEngine._apply_to_dataframe_parallel(
                execution_graph,
                data,
                online,
                n_processes,
                engine_type,
                column_store,
                merge,
            )

        # Merge the accumulated outputs back into the frame, then project to the
        # surviving columns in topological order.
        if column_store:
            exec_engine = engine._get_instance()
            overwritten = set(column_store.keys()) & set(data.columns)
            base = exec_engine._drop_columns(data, overwritten) if overwritten else data
            result_df = (
                pl.DataFrame(column_store) if is_polars else pd.DataFrame(column_store)
            )
            # Workers rebuild the DataFrame from Arrow shared memory with
            # preserve_index=False, so parallel-path results carry a fresh
            # RangeIndex while `data` keeps the caller's index. pd.concat aligns by
            # index, which would NaN-pad every row on mismatch. Row order is
            # preserved end to end, so realign positionally.
            if isinstance(result_df, pd.DataFrame) and not result_df.index.equals(
                data.index
            ):
                result_df.index = data.index
            data = exec_engine._concat_dataframes([base, result_df])
        return data[[c for c in column_order if c not in dropped_features]]

    @staticmethod
    def _apply_to_dataframe_parallel(
        execution_graph: TransformationExecutionDAG,
        data: pd.DataFrame | pl.DataFrame,
        online: bool,
        n_processes: int,
        engine_type: str,
        column_store: dict[str, Any],
        merge: Callable[[transformation_function.TransformationFunction, Any], None],
    ) -> None:
        """Run the DataFrame DAG in parallel, staging the frame in shared memory.

        The frame is written once to Arrow shared memory; each worker reads only
        the columns its transformation needs, with predecessor outputs passed
        through so a chained transformation sees its inputs. Results are merged
        into ``column_store`` via ``merge``.
        """
        TransformationFunctionEngine._ensure_pool(n_processes)
        pool = TransformationFunctionEngine.__process_pool

        use_shm = HAS_PYARROW
        shm_ref: shared_memory.SharedMemory | None = None
        shm_name = shm_size = is_polars = None
        if use_shm:
            try:
                shm_ref, shm_size, is_polars = (
                    TransformationFunctionEngine._write_to_shared_memory(data)
                )
                shm_name = shm_ref.name
            except OSError as e:
                # ENOSPC/ENOMEM downgrades to pickle IPC instead of bubbling up an
                # opaque OSError.
                if e.errno not in (errno.ENOSPC, errno.ENOMEM):
                    raise
                _logger.warning(
                    "Shared memory unavailable (errno=%s); falling back to pickle "
                    "IPC. Consider increasing /dev/shm size on the host.",
                    e.errno,
                )
                use_shm = False
                shm_ref = shm_name = shm_size = is_polars = None

        def submit(tf):
            needed = tf.hopsworks_udf.transformation_features
            udf_kwargs: dict[str, Any] = {
                "udf": tf.hopsworks_udf,
                "online": online,
                "engine_type": engine_type,
            }
            if use_shm:
                predecessor_cols = {
                    c: column_store[c] for c in needed if c in column_store
                }
                udf_kwargs.update(
                    shm_name=shm_name,
                    shm_size=shm_size,
                    is_polars=is_polars,
                    columns=needed,
                    predecessor_columns=predecessor_cols or None,
                )
            else:
                udf_kwargs["data"] = data
            return pool.submit(TransformationFunctionEngine._execute_udf, **udf_kwargs)

        try:
            TransformationFunctionEngine._schedule_dag(execution_graph, submit, merge)
        finally:
            # Unlink only after the schedule completes or its cleanup has drained
            # in-flight workers, so no worker reads a released segment.
            if shm_ref is not None:
                try:
                    shm_ref.close()
                    shm_ref.unlink()
                except FileNotFoundError:
                    pass

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
            for row, rq in zip(transformed_data, request_parameters, strict=False):
                row.update(rq)
        else:
            for row in transformed_data:
                row.update(request_parameters)
        return transformed_data

    @staticmethod
    def _schedule_dag(
        execution_graph: TransformationExecutionDAG,
        submit: Callable[[transformation_function.TransformationFunction], Any],
        collect: Callable[[transformation_function.TransformationFunction, Any], None],
    ) -> None:
        """Drive a streaming DAG schedule over the process pool.

        ``submit(tf)`` submits one transformation's work and returns its future;
        ``collect(tf, result)`` consumes that future's result. A transformation is
        submitted as soon as its predecessors complete (``wait(FIRST_COMPLETED)``),
        so independent branches run concurrently and a dependent starts the moment
        it unblocks. On any worker failure the pending futures are cancelled,
        in-flight ones drained (so no worker is mid-read of state the caller is
        about to release), and the singleton pool discarded before re-raising.
        """
        id_to_tf = {id(tf): tf for tf in execution_graph.nodes}
        sorter = execution_graph._new_topological_sorter()
        future_to_tf: dict[Any, int] = {}
        try:
            while sorter.is_active():
                for tf_id in sorter.get_ready():
                    future_to_tf[submit(id_to_tf[tf_id])] = tf_id
                done, _ = wait(future_to_tf.keys(), return_when=FIRST_COMPLETED)
                for future in done:
                    tf_id = future_to_tf.pop(future)
                    collect(id_to_tf[tf_id], future.result())
                    sorter.done(tf_id)
        except (Exception, BrokenProcessPool):
            for f in future_to_tf:
                f.cancel()
            if future_to_tf:
                wait(future_to_tf.keys(), return_when=ALL_COMPLETED)
            TransformationFunctionEngine._shutdown_process_pool(
                wait=False, cancel_futures=True
            )
            raise

    @staticmethod
    def _ensure_pool(n_processes: int) -> None:
        """Build the singleton process pool, reusing it when the worker count matches.

        An absent pool is built; a pool with a different ``n_processes`` is torn
        down and replaced; a matching pool is reused.
        """
        if (
            TransformationFunctionEngine.__process_pool is not None
            and TransformationFunctionEngine.__process_pool_n_processes == n_processes
        ):
            return
        # Replace a mismatched pool (no-op when absent), then build.
        TransformationFunctionEngine._shutdown_process_pool()
        mp_context = multiprocessing.get_context(
            TransformationFunctionEngine._resolve_mp_start_method()
        )
        TransformationFunctionEngine.__process_pool = ProcessPoolExecutor(
            max_workers=n_processes,
            mp_context=mp_context,
            initializer=TransformationFunctionEngine._init_worker,
            initargs=(engine._get_type(),),
        )
        TransformationFunctionEngine.__process_pool_n_processes = n_processes

    @staticmethod
    def _write_to_shared_memory(
        dataframe: pd.DataFrame,
    ) -> tuple[shared_memory.SharedMemory, int, bool]:
        """Serialize a DataFrame to Arrow IPC in shared memory.

        Returns (shm, buffer_size, is_polars). Caller must call
        shm.close() and shm.unlink() after all workers are done.
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
        buf = sink.getvalue()  # pa.Buffer, no Python bytes copy

        shm = shared_memory.SharedMemory(create=True, size=buf.size)
        try:
            shm.buf[: buf.size] = memoryview(buf).cast("B")  # single C-level memcpy
        except BaseException:
            # If the memcpy raises (MemoryError, OSError, etc.) the SharedMemory
            # segment exists but the caller will never see it. Release it here
            # so we don't leak /dev/shm/psm_* segments on partial failure.
            # Best-effort cleanup that must NOT mask the original failure, so each
            # step is wrapped independently and any cleanup error is logged.
            try:
                shm.close()
            except Exception as cleanup_exc:
                _logger.warning(
                    "shm.close() failed during shared-memory allocation rollback: %s",
                    cleanup_exc,
                )
            try:
                shm.unlink()
            except Exception as cleanup_exc:
                _logger.warning(
                    "shm.unlink() failed during shared-memory allocation rollback: %s",
                    cleanup_exc,
                )
            raise
        # Do NOT close here: on Windows the mapping is destroyed when the
        # last handle is closed.  The caller must keep this object alive and
        # call close()/unlink() after all workers are done.
        return shm, buf.size, is_polars

    @staticmethod
    def _read_from_shared_memory(
        shm_name: str,
        shm_size: int,
        as_polars: bool = False,
    ) -> pd.DataFrame:
        """Deserialize a DataFrame from Arrow IPC in shared memory."""
        shm = shared_memory.SharedMemory(name=shm_name, create=False)
        try:
            if as_polars:
                # Polars retains zero-copy references to Arrow memory, so we
                # must copy the bytes out of shared memory before closing it.
                reader = ipc.open_stream(bytes(shm.buf[:shm_size]))
                result = pl.from_arrow(reader.read_all())
            else:
                # Wrap shared memory as an Arrow buffer for a zero-copy read.
                # to_pandas() copies into numpy arrays, releasing Arrow refs.
                buf = pa.py_buffer(shm.buf[:shm_size])
                table = ipc.open_stream(buf).read_all()
                result = table.to_pandas()
                del table, buf
        finally:
            shm.close()
        return result

    @staticmethod
    def _execute_udf(
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
        """Execute a single UDF on the given data.

        When `shm_name` is provided the DataFrame is read from Arrow shared
        memory instead of being deserialized via pickle.

        Parameters:
            udf: The transformation function to execute.
            data: Input data to transform.
                Can be a DataFrame, a single dict, or a list of dicts.
            engine_type: Engine type override (`"python"` or `"spark"`).
            online: Whether to apply online-mode transformations.
            shm_name: Name of the shared-memory block holding an Arrow IPC stream.
                When set, `data` is ignored and the DataFrame is read from shared memory.
            shm_size: Size in bytes of the shared-memory payload.
            is_polars: If `True`, deserialize the shared-memory payload as a Polars DataFrame.
            columns: Subset of columns to select from the shared-memory DataFrame.
            predecessor_columns: Column values produced by predecessor UDFs that override
                columns read from shared memory.

        Returns:
            The transformed data in the same container type as the input
            (dict, list of dicts, or DataFrame).
        """
        if shm_name is not None:
            data = TransformationFunctionEngine._read_from_shared_memory(
                shm_name, shm_size, is_polars
            )
            if columns:
                col_data = {
                    c: predecessor_columns[c]
                    if predecessor_columns and c in predecessor_columns
                    else data[c]
                    for c in columns
                }
                data = pl.DataFrame(col_data) if is_polars else pd.DataFrame(col_data)

        # Check dict/list first: these are the dominant types on the online
        # serving hot path and avoid the multiple isinstance() checks inside
        # _check_supported_dataframe() that all fail for non-DataFrame types.
        if isinstance(data, dict):
            return TransformationFunctionEngine._apply_udf_on_dict(
                udf=udf, data=data, online=online, engine_type=engine_type
            )
        if isinstance(data, list):
            return [
                TransformationFunctionEngine._apply_udf_on_dict(
                    udf=udf, data=row, online=online, engine_type=engine_type
                )
                for row in data
            ]
        execution_engine = engine._get_instance()
        if execution_engine._check_supported_dataframe(data):
            return execution_engine._apply_udf_on_dataframe(
                udf=udf, dataframe=data, online=online, engine_type=engine_type
            )
        raise exceptions.FeatureStoreException(
            f"Dataframe type {type(data)} not supported in the engine."
        )

    @staticmethod
    def _apply_udf_on_dict(
        udf: HopsworksUdf,
        data: dict[str, Any],
        online: bool | None = True,
        engine_type: str | None = None,
    ) -> dict[str, Any]:
        """Apply the UDF of a transformation function on a single dictionary record.

        This function is not pushed to the Python Engine since it needs to be
        executable in both the Python and Spark kernels.

        This is an internal helper. It returns a dict containing only the
        transformed output columns, not the merged input plus outputs.
        Callers within the engine merge the result back into the source row
        themselves so the DAG scheduler can feed a fresh per-row _snapshot to
        each TF. External callers should not depend on the result carrying
        the full row.

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
            udf.execution_mode._get_current_execution_mode(online=online)
            == UDFExecutionMode.PANDAS
        )

        prefix = udf.feature_name_prefix
        features = []
        for feat in udf.unprefixed_transformation_features:
            feature_name = prefix + feat if prefix else feat
            val = data[feature_name] if feature_name in data else data[feat]
            features.append(pd.Series([val], name=feat) if is_pandas_mode else val)

        transformed_result = udf._get_udf(online=online, engine_type=engine_type)(
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

    def _delete(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> None:
        """Delete a transformation function from the feature store.

        Parameters:
            transformation_function_instance: The transformation function to be removed from the feature store.
        """
        self._transformation_function_api._delete(transformation_function_instance)

    @staticmethod
    def _compute_transformation_fn_statistics(
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
        return training_dataset_obj._statistics_engine._compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=statistics_features,
            label_encoder_features=label_encoder_features,  # label encoded features only
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def _get_ready_to_use_transformation_fns(
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
                    "Training data version is required for transformation. Call `feature_view._init_serving(version)` "
                    "or `feature_view._init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
                )
            td_tffn_stats = feature_view._statistics_engine._get(
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
    def _fit_and_transform(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        dataset: dict[
            str, pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame")
        ]
        | pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        transformation_context: dict[str, Any] | None = None,
        n_processes: int | None = None,
        training_dataset_version: int | None = None,
    ) -> (
        dict[str, pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame")]
        | pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame")
    ):
        """Bind transformation statistics and return the transformed dataset.

        The single entry point for training data creation: statistics are
        retrieved from the backend when `training_dataset_version` is given,
        otherwise fit on the train data, and the model-dependent transformations
        are applied exactly once to every frame.

        Parameters:
            training_dataset: The training dataset being created.
            feature_view_obj: The feature view the training data is created from.
            dataset: A single dataframe, or a dictionary of split dataframes.
            transformation_context: Context variables passed to the transformation functions.
            n_processes: Number of worker processes for applying the transformations.
            training_dataset_version: When given, statistics are retrieved from the backend for this version instead of being fit.

        Returns:
            The transformed dataset, in the same shape as `dataset`.
        """
        if training_dataset_version is not None:
            # The statistics of an existing training dataset version already
            # exist in the backend: bind them, then every frame (train
            # included) gets a plain transform.
            TransformationFunctionEngine._get_and_set_feature_statistics(
                training_dataset, feature_view_obj, training_dataset_version
            )
            if not isinstance(dataset, dict):
                return TransformationFunctionEngine._transform(
                    feature_view_obj, dataset, transformation_context, n_processes
                )
            return {
                split_name: TransformationFunctionEngine._transform(
                    feature_view_obj, frame, transformation_context, n_processes
                )
                for split_name, frame in dataset.items()
            }

        if not isinstance(dataset, dict):
            return TransformationFunctionEngine._fit_transform_train(
                training_dataset,
                feature_view_obj,
                dataset,
                transformation_context=transformation_context,
                n_processes=n_processes,
            )

        # The train split is fit on and transformed first: fitting binds the
        # statistics the remaining splits are transformed with.
        transformed_splits = {}
        if training_dataset.train_split in dataset:
            transformed_splits[training_dataset.train_split] = (
                TransformationFunctionEngine._fit_transform_train(
                    training_dataset,
                    feature_view_obj,
                    dataset[training_dataset.train_split],
                    transformation_context=transformation_context,
                    n_processes=n_processes,
                )
            )
        for split_name, frame in dataset.items():
            if split_name != training_dataset.train_split:
                transformed_splits[split_name] = (
                    TransformationFunctionEngine._transform(
                        feature_view_obj, frame, transformation_context, n_processes
                    )
                )
        return transformed_splits

    @staticmethod
    def _transform(
        feature_view_obj: feature_view.FeatureView,
        feature_dataframe: pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        transformation_context: dict[str, Any] | None,
        n_processes: int | None,
    ) -> pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame"):
        """Apply the feature view's model-dependent transformations to one frame.

        A plain offline transform: the transformation statistics must already
        be bound (fit on the train data or fetched from the backend).

        Parameters:
            feature_view_obj: The feature view whose transformations are applied.
            feature_dataframe: The frame to transform.
            transformation_context: Context variables passed to the transformation functions.
            n_processes: Number of worker processes for applying the transformations.

        Returns:
            The transformed frame.
        """
        return TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=feature_view_obj._model_dependent_transformation_execution_graph,
            data=feature_dataframe,
            online=False,
            transformation_context=transformation_context,
            n_processes=n_processes,
        )

    @staticmethod
    def _fit_transform_train(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        feature_dataframe: pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        transformation_context: dict[str, Any] | None = None,
        n_processes: int | None = None,
    ) -> pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame"):
        """Fit the required statistics on the train data and return it transformed.

        sklearn `fit_transform` semantics: the returned frame is always the
        transformed input, and the fitted statistics are bound to every
        transformation function and persisted in a single save. Only the source
        of the statistics differs per case:

        - no statistic required: plain transform;
        - statistics on raw features only: one profiling pass, then a plain
          transform;
        - statistics on chained (intermediate) features: profiling needs the
          producing transformations executed first, so fitting and transforming
          interleave in one fused pass over the DAG.

        Statistics are fit on the train data only, so the transformations never
        see the test rows and there is no leakage.

        Parameters:
            training_dataset: The training dataset the statistics are persisted for.
            feature_view_obj: The feature view whose transformation functions are fit and applied.
            feature_dataframe: The train data.
            transformation_context: Context variables passed to the transformation functions.
            n_processes: Number of worker processes for applying the transformations.

        Returns:
            The transformed train data.
        """
        statistics_features: set[str] = set()
        label_encoder_features: set[str] = set()
        for tf in feature_view_obj.transformation_functions:
            statistics_features.update(tf.hopsworks_udf.statistics_features)
            if tf.hopsworks_udf.function_name in ("label_encoder", "one_hot_encoder"):
                label_encoder_features.update(tf.hopsworks_udf.statistics_features)

        # A statistic required on a feature that is itself produced by another
        # transformation function (an intermediate of a chain, e.g. the imputed
        # column consumed by a scaler) does not exist in the raw data; fitting
        # it requires executing the producing transformations first.
        produced_features = {
            output_feature
            for tf in feature_view_obj.transformation_functions
            for output_feature in tf.hopsworks_udf.output_column_names
        }
        if statistics_features & produced_features:
            return TransformationFunctionEngine._fit_transform_chained(
                training_dataset,
                feature_view_obj,
                feature_dataframe,
                statistics_features,
                label_encoder_features,
                transformation_context,
                n_processes,
            )

        if statistics_features:
            # Every required statistic is on a raw feature: one profiling pass
            # before the transform.
            stats = TransformationFunctionEngine._compute_transformation_fn_statistics(
                training_dataset,
                list(statistics_features),
                list(label_encoder_features),
                feature_dataframe,
                feature_view_obj,
            )
            for tf in feature_view_obj.transformation_functions:
                tf.transformation_statistics = stats.feature_descriptive_statistics

        return TransformationFunctionEngine._transform(
            feature_view_obj, feature_dataframe, transformation_context, n_processes
        )

    @staticmethod
    def _fit_transform_chained(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        feature_dataframe: pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        statistics_features: set[str],
        label_encoder_features: set[str],
        transformation_context: dict[str, Any] | None,
        n_processes: int | None,
    ) -> pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame"):
        """Fit statistics over a chained DAG and return the transformed data.

        Fitting a statistic on an intermediate feature requires executing its
        producing transformations, so fitting and transforming are fused into a
        single pass (sklearn Pipeline.fit_transform semantics): the DAG is
        consumed in stages delimited by the statistics barriers, each stage's
        statistics are profiled in one pass, then the stage runs as one sub-DAG
        (keeping node-parallelism and Spark plan pushdown). Every transformation
        executes exactly once on the train data.

        Parameters:
            training_dataset: The training dataset the statistics are persisted for.
            feature_view_obj: The feature view whose transformation functions are applied.
            feature_dataframe: The train data to fit on and transform.
            statistics_features: All features the transformation functions require statistics on.
            label_encoder_features: The subset of `statistics_features` consumed by encoders.
            transformation_context: Context variables passed to the transformation functions.
            n_processes: Number of worker processes for applying each stage.

        Returns:
            The fully transformed train data.
        """
        graph = TransformationExecutionDAG(feature_view_obj.transformation_functions)
        is_spark_dataframe = engine._get_type() == "spark"
        # Resolved once against the full DAG so the cap warning reflects what
        # the user requested; the stages below are internal slices and are
        # narrowed silently.
        n_processes = TransformationFunctionEngine._resolve_n_processes(
            graph, n_processes
        )

        provisional_statistics: list = []
        provisioned_features: set[str] = set()
        working_dataframe = feature_dataframe
        original_columns = list(feature_dataframe.columns)
        statistics_engine = training_dataset._statistics_engine
        previous_barrier = None

        remaining = list(graph.nodes)  # topological order
        while remaining:
            stage, remaining = TransformationFunctionEngine._next_statistics_stage(
                remaining, provisioned_features
            )
            features_to_fit = {
                f for tf in stage for f in tf.hopsworks_udf.statistics_features
            } - provisioned_features
            if features_to_fit:
                # Present in working_dataframe: raw features from the start,
                # intermediate features materialized by earlier stages. Profiled
                # without persisting; the stage transformations only need the
                # values to run, and the authoritative statistics are persisted
                # once below.
                if is_spark_dataframe:
                    working_dataframe = (
                        TransformationFunctionEngine._persist_statistics_barrier(
                            working_dataframe, previous_barrier
                        )
                    )
                    previous_barrier = working_dataframe
                provisional = (
                    statistics_engine._compute_transformation_fn_statistics_no_save(
                        list(features_to_fit),
                        list(label_encoder_features & features_to_fit),
                        working_dataframe,
                    )
                )
                provisional_statistics.extend(
                    provisional.feature_descriptive_statistics
                )
                provisioned_features |= features_to_fit
            for tf in stage:
                tf.transformation_statistics = provisional_statistics
            # expected_features keeps the already-present columns so a dropping
            # transformation does not remove inputs later stages still need;
            # drops are applied once in the final projection below.
            stage_graph = TransformationExecutionDAG(stage)
            working_dataframe = (
                TransformationFunctionEngine._apply_transformation_functions(
                    execution_graph=stage_graph,
                    data=working_dataframe,
                    online=False,
                    transformation_context=transformation_context,
                    expected_features=set(working_dataframe.columns),
                    n_processes=min(n_processes, stage_graph.max_parallelism),
                )
            )

        # Every required statistic feature (raw and intermediate) is now a
        # column of working_dataframe. Compute and persist them in a single save
        # so serving retrieves one complete statistics entity (not the most
        # recent partial one).
        if is_spark_dataframe:
            working_dataframe = (
                TransformationFunctionEngine._persist_statistics_barrier(
                    working_dataframe, previous_barrier
                )
            )
        stats = TransformationFunctionEngine._compute_transformation_fn_statistics(
            training_dataset,
            list(statistics_features),
            list(label_encoder_features),
            working_dataframe,
            feature_view_obj,
        )
        for tf in feature_view_obj.transformation_functions:
            tf.transformation_statistics = stats.feature_descriptive_statistics

        # Project to the layout a plain transform pass would have produced:
        # deferred drops applied once, surviving input columns first,
        # transformation outputs appended in topological order.
        dropped_features, column_order = (
            TransformationFunctionEngine._transformed_column_layout(
                graph, original_columns, None
            )
        )
        final_columns = [c for c in column_order if c not in dropped_features]
        if is_spark_dataframe:
            return working_dataframe.select(*final_columns)
        return working_dataframe[final_columns]

    @staticmethod
    def _next_statistics_stage(
        remaining: list[transformation_function.TransformationFunction],
        provisioned_features: set[str],
    ) -> tuple[
        list[transformation_function.TransformationFunction],
        list[transformation_function.TransformationFunction],
    ]:
        """Split the next statistics stage off a topologically ordered list.

        A stage is the longest prefix whose required statistics are not produced
        by a transformation within the prefix itself (the next statistics
        barrier), so all of its statistics can be profiled in one pass before
        the whole stage runs.

        Parameters:
            remaining: Transformation functions not yet staged, in topological order.
            provisioned_features: Features whose statistics are already fit.

        Returns:
            The stage and the remaining transformation functions after it.
        """
        stage: list = []
        stage_outputs: set[str] = set()
        for tf in remaining:
            needed = set(tf.hopsworks_udf.statistics_features) - provisioned_features
            if needed & stage_outputs:
                break
            stage.append(tf)
            stage_outputs.update(tf.hopsworks_udf.output_column_names)
        return stage, remaining[len(stage) :]

    @staticmethod
    def _persist_statistics_barrier(
        dataframe: TypeVar("pyspark.sql.DataFrame"),
        previous_barrier: TypeVar("pyspark.sql.DataFrame") | None,
    ) -> TypeVar("pyspark.sql.DataFrame"):
        """Persist a Spark statistics barrier and release the previous one.

        Spark dataframes are lazy and every statistics profiling call is an
        action; without a checkpoint each action re-executes the lineage from
        the source read. Persisting at each barrier makes every later action
        read the checkpoint instead, so at most two checkpoints are alive and
        each action only computes the plan suffix since the last barrier. The
        final barrier backs the training dataset write and is evicted by Spark
        afterwards.

        Parameters:
            dataframe: The frame to persist as the new barrier.
            previous_barrier: The barrier to release, if any.

        Returns:
            The persisted frame.
        """
        from pyspark import StorageLevel

        dataframe = dataframe.persist(StorageLevel.MEMORY_AND_DISK)
        if previous_barrier is not None:
            previous_barrier.unpersist()
        return dataframe

    @staticmethod
    def _get_and_set_feature_statistics(
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
            td_tffn_stats = training_dataset._statistics_engine._get(
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


# Register pool shutdown once at module import. Registering per engine
# instance would leak one atexit hook per construction.
atexit.register(TransformationFunctionEngine._shutdown_process_pool)

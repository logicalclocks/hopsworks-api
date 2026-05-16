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
import sys
import warnings
from concurrent.futures import (
    ALL_COMPLETED,
    FIRST_COMPLETED,
    ProcessPoolExecutor,
    wait,
)
from concurrent.futures.process import BrokenProcessPool
from graphlib import TopologicalSorter
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


if HAS_POLARS:
    import polars as pl

if HAS_PYARROW:
    import pyarrow as pa
    import pyarrow.ipc as ipc


if TYPE_CHECKING:
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

    __process_pool: ProcessPoolExecutor | None = None
    __process_pool_n_processes: int | None = None

    def __init__(self, feature_store_id: int):
        self._feature_store_id = feature_store_id
        self._transformation_function_api: transformation_function_api.TransformationFunctionApi = transformation_function_api.TransformationFunctionApi(
            feature_store_id
        )

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
        TransformationFunctionEngine.__process_pool_n_processes = None

    @staticmethod
    def _init_worker(engine_type: str):
        """Initialize the engine singleton in a worker process (needed for spawn)."""
        global _IS_TF_WORKER
        _IS_TF_WORKER = True
        import hopsworks_common.connection

        hopsworks_common.connection._hsfs_engine_type = engine_type
        engine.init(engine_type)

    @staticmethod
    def warmup_online_workers(n_processes: int) -> None:
        """Pre-spawn the transformation function worker pool.

        Latency-sensitive deployments should call this once at startup so the
        first `get_feature_vector(s)` call does not pay process spawn and
        engine-init latency.
        Subsequent calls with a different `n_processes` will replace the pool.

        Parameters:
            n_processes: Number of worker processes to pre-spawn.
        """
        if n_processes < 1:
            raise exceptions.FeatureStoreException(
                "warmup_online_workers requires n_processes >= 1."
            )
        TransformationFunctionEngine.create_process_pool(n_processes)

    @staticmethod
    def create_process_pool(n_processes: int | None = None):
        """Create (or replace) a process pool for parallel TF execution.

        Parameters:
            n_processes: Maximum number of worker processes.
                Defaults to the number of CPUs when `None`.
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
        TransformationFunctionEngine.__process_pool_n_processes = n_processes

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

        if is_dataframe:
            data_features = set(data.columns)
        elif isinstance(data, list):
            # list[dict]: use the first row's keys; later rows are assumed to
            # share the same shape (callers build these uniformly).
            data_features = set(data[0].keys()) if data else set()
        else:
            data_features = set(data.keys())
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
        execution_graph: TransformationExecutionDAG | None = None,
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any] = None,
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] | None = None,
        request_parameters: dict[str, Any] | None = None,
        expected_features: set[str] | None = None,
        n_processes: int | None = None,
        transformation_functions: list[transformation_function.TransformationFunction]
        | None = None,
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
            transformation_functions: Deprecated; pass `execution_graph` instead.
                Accepts a flat list of transformation functions; the engine builds a DAG internally.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if execution_graph is not None and transformation_functions is not None:
            raise exceptions.FeatureStoreException(
                "Pass either `execution_graph` or `transformation_functions`, not both. "
                "`transformation_functions` is deprecated; prefer `execution_graph`."
            )
        if execution_graph is None and transformation_functions is None:
            raise exceptions.FeatureStoreException(
                "One of `execution_graph` or `transformation_functions` must be provided."
            )
        if transformation_functions is not None:
            warnings.warn(
                "`transformation_functions=` is deprecated and will be removed in a future release. "
                "Pass `execution_graph=TransformationExecutionDAG(transformation_functions)` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            execution_graph = TransformationExecutionDAG(transformation_functions)

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
        # Collect all TF output columns in topo order for final column ordering.
        tf_output_cols: list[str] = []
        tf_output_set: set[str] = set()

        for tf in execution_graph.nodes:
            udf = tf.hopsworks_udf
            udf.transformation_context = transformation_context
            if udf.dropped_features:
                dropped_features.update(
                    {f for f in udf.dropped_features if f not in expected_features}
                    if expected_features
                    else udf.dropped_features
                )
            for col in udf.output_column_names:
                tf_output_set.add(col)
                tf_output_cols.append(col)

        if is_dataframe:
            # Original columns (minus those overwritten by TFs) + TF outputs in topo order
            column_order = [
                c for c in data.columns if c not in tf_output_set
            ] + tf_output_cols
        if request_parameters:
            data = TransformationFunctionEngine._update_request_parameter_data(
                data, request_parameters
            )

        # --- Dict/list: sequential, topo order, in-place update ---
        if isinstance(data, (dict, list)):
            rows = (
                [row.copy() for row in data]
                if isinstance(data, list)
                else [data.copy()]
            )
            eng_type = engine.get_type()
            for tf in execution_graph.nodes:
                for row in rows:
                    row.update(
                        TransformationFunctionEngine.execute_udf(
                            udf=tf.hopsworks_udf,
                            data=row,
                            online=online,
                            engine_type=eng_type,
                        )
                    )
            cleaned = [
                {k: v for k, v in row.items() if k not in dropped_features}
                for row in rows
            ]
            return cleaned if isinstance(data, list) else cleaned[0]

        # --- DataFrame: sequential (n_processes==1) or parallel DAG ---
        column_store = {}  # col_name -> Series (accumulated results from completed TFs)

        # M3: pool overhead dominates for small DAGs or small inputs, so default
        # to sequential unless the workload justifies a pool. Callers can always
        # force parallelism by passing n_processes explicitly.
        if n_processes is None:
            if len(execution_graph.nodes) < 2 or len(data) < 10_000:
                n_processes = 1
            else:
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
            if (
                TransformationFunctionEngine.__process_pool is None
                or TransformationFunctionEngine.__process_pool_n_processes
                != n_processes
            ):
                TransformationFunctionEngine.create_process_pool(n_processes)
            pool = TransformationFunctionEngine.__process_pool

            id_to_tf = {id(tf): tf for tf in execution_graph.nodes}
            ts = TopologicalSorter(
                {
                    id(tf): [id(dep) for dep in execution_graph.dependencies[id(tf)]]
                    for tf in execution_graph.nodes
                }
            )
            ts.prepare()
            future_to_tf: dict[Any, int] = {}

            use_shm = HAS_PYARROW
            shm_ref: shared_memory.SharedMemory | None = None
            shm_name = shm_size = is_polars = None
            try:
                # B2: shm allocation lives inside the try; ENOSPC/ENOMEM downgrades
                # to pickle IPC instead of bubbling up an opaque OSError.
                if use_shm:
                    try:
                        shm_ref, shm_size, is_polars = (
                            TransformationFunctionEngine._write_to_shared_memory(data)
                        )
                        shm_name = shm_ref.name
                    except OSError as e:
                        if e.errno not in (errno.ENOSPC, errno.ENOMEM):
                            raise
                        _logger.warning(
                            "Shared memory unavailable (errno=%s) — falling back to "
                            "pickle IPC. Consider increasing /dev/shm size on the host.",
                            e.errno,
                        )
                        use_shm = False
                        shm_ref = None
                        shm_name = None
                        shm_size = None
                        is_polars = None

                try:
                    while ts.is_active():
                        for tf_id in ts.get_ready():
                            tf = id_to_tf[tf_id]
                            needed = tf.hopsworks_udf.transformation_features
                            udf_kwargs: dict[str, Any] = {
                                "udf": tf.hopsworks_udf,
                                "online": online,
                                "engine_type": engine.get_type(),
                            }
                            if use_shm:
                                predecessor_cols = {
                                    c: column_store[c]
                                    for c in needed
                                    if c in column_store
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
                            future = pool.submit(
                                TransformationFunctionEngine.execute_udf,
                                **udf_kwargs,
                            )
                            future_to_tf[future] = tf_id

                        done, _ = wait(
                            future_to_tf.keys(), return_when=FIRST_COMPLETED
                        )
                        for future in done:
                            tf_id = future_to_tf.pop(future)
                            result = future.result()
                            for col in result.columns:
                                column_store[col] = result[col]
                            ts.done(tf_id)
                except (Exception, BrokenProcessPool) as original_exc:
                    # B3 + M6: single cleanup path for recoverable UDF failures
                    # and BrokenProcessPool. Cancel pending, drain in-flight so no
                    # worker is reading shm we are about to unlink, then null the
                    # singleton pool so the next call lazily builds a fresh one.
                    for f in future_to_tf:
                        f.cancel()
                    if future_to_tf:
                        wait(future_to_tf.keys(), return_when=ALL_COMPLETED)
                    TransformationFunctionEngine.__process_pool = None
                    TransformationFunctionEngine.__process_pool_n_processes = None
                    raise original_exc
            finally:
                if shm_ref is not None:
                    try:
                        shm_ref.close()
                        shm_ref.unlink()
                    except FileNotFoundError:
                        pass

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
            for row, rq in zip(transformed_data, request_parameters, strict=False):
                row.update(rq)
        else:
            for row in transformed_data:
                row.update(request_parameters)
        return transformed_data

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
        buf = sink.getvalue()  # pa.Buffer — no Python bytes copy

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
        """Deserialize a DataFrame from Arrow IPC in shared memory."""
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

        prefix = udf.feature_name_prefix
        features = []
        for feat in udf.unprefixed_transformation_features:
            feature_name = prefix + feat if prefix else feat
            val = data[feature_name] if feature_name in data else data[feat]
            features.append(pd.Series([val], name=feat) if is_pandas_mode else val)

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


# Module-scope: register pool shutdown exactly once on import (M6).
# Previously called from `__init__` which leaked one hook per engine instance.
atexit.register(TransformationFunctionEngine.shutdown_process_pool)

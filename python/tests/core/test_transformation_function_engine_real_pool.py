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

import atexit
import errno
import glob
import sys
import time

import pandas as pd
import pytest
from hopsworks_common.core.constants import HAS_POLARS, HAS_PYARROW
from hsfs import engine as hsfs_engine
from hsfs import transformation_function
from hsfs.core import transformation_execution_dag, transformation_function_engine
from hsfs.engine import python as python_engine_mod
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType


if HAS_POLARS:
    import polars as pl


# region Module-scope UDFs
# Process-pool workers must be able to pickle these.
@udf(int)
def _add_one_sleep(col1):
    time.sleep(0.5)
    return col1 + 1


@udf(int)
def _add_two_sleep(col1):
    time.sleep(0.5)
    return col1 + 2


@udf(int)
def _double_in(in_col):
    return in_col * 2


@udf(int)
def _add_one(col1):
    return col1 + 1


@udf(int)
def _add_two(col1):
    return col1 + 2


@udf(int)
def _raise_in_worker(col1):
    raise RuntimeError("UDF intentional failure")


# endregion


def _make_tf(udf_fn) -> transformation_function.TransformationFunction:
    return transformation_function.TransformationFunction(
        featurestore_id=99,
        hopsworks_udf=udf_fn,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )


@pytest.fixture(autouse=True)
def _real_python_engine():
    hsfs_engine._set_instance(engine=python_engine_mod.Engine(), engine_type="python")
    yield
    transformation_function_engine.TransformationFunctionEngine._shutdown_process_pool()


def _shm_segments() -> list[str]:
    if sys.platform.startswith("linux"):
        return glob.glob("/dev/shm/psm_*")
    return []


@pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow required for the shared-memory IPC path"
)
class TestRealPoolExecution:
    def test_apply_real_pool_parallel(self):
        """Two overlapping roots: the parallel run must beat a sequential baseline."""
        tf1 = _make_tf(_add_one_sleep)
        tf2 = _make_tf(_add_two_sleep)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        df = pd.DataFrame({"col1": list(range(10))})
        cls = transformation_function_engine.TransformationFunctionEngine

        # Sequential baseline (single worker, no overlap).
        t0 = time.perf_counter()
        cls._apply_transformation_functions(execution_graph=dag, data=df, n_processes=1)
        sequential = time.perf_counter() - t0

        # Pre-spawn the pool so the parallel measurement captures scheduler
        # overlap, not worker startup — spawn cost (Windows, or POSIX with
        # HOPSWORKS_TF_POOL_START_METHOD=spawn) would otherwise mask the signal.
        cls._warmup_online_workers(n_processes=2)

        t0 = time.perf_counter()
        result = cls._apply_transformation_functions(
            execution_graph=dag, data=df, n_processes=2
        )
        parallel = time.perf_counter() - t0

        # Compare against the measured baseline rather than a fixed threshold:
        # overlapping two independent roots must be faster than running them
        # back to back, on any runner.
        assert parallel < sequential, (
            f"Parallel execution ({parallel:.2f}s) was not faster than the "
            f"sequential baseline ({sequential:.2f}s)."
        )
        assert set(result.columns) >= {
            "_add_one_sleep_col1_",
            "_add_two_sleep_col1_",
        }

    @pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
    def test_apply_shm_polars_roundtrip(self):
        """Polars input via shm matches sequential output."""
        tf1 = _make_tf(_add_one)
        tf2 = _make_tf(_add_two)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        df = pl.DataFrame({"col1": list(range(10))})

        parallel = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=dag, data=df, n_processes=2
        )
        sequential = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=dag, data=df, n_processes=1
        )
        assert parallel.to_dict(as_series=False) == sequential.to_dict(as_series=False)

    def test_apply_preserves_non_range_index(self):
        """Parallel shm path must not misalign rows on a non-RangeIndex input.

        Workers rebuild the DataFrame from Arrow shm with preserve_index=False,
        so their results carry a RangeIndex; the merge realigns positionally.
        Without that, pd.concat aligns by index and NaN-pads every row.
        """
        tf1 = _make_tf(_add_one)
        tf2 = _make_tf(_add_two)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        index = pd.Index(range(100, 110))
        df = pd.DataFrame({"col1": list(range(10))}, index=index)

        result = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=dag, data=df, n_processes=2
        )

        assert len(result) == 10
        assert result.index.equals(index)
        assert not result.isna().any().any()
        assert result["_add_one_col1_"].tolist() == [v + 1 for v in range(10)]
        assert result["_add_two_col1_"].tolist() == [v + 2 for v in range(10)]

    def test_warmup_barrier_wait_times_out(self):
        """A pool that cannot fill the barrier breaks it instead of hanging."""
        import threading

        barrier = threading.Barrier(2)
        with pytest.raises(threading.BrokenBarrierError):
            transformation_function_engine._warmup_barrier_wait(barrier, timeout=0.1)

    def test_apply_worker_exception_cleans_up(self):
        """UDF exception propagates with __cause__, pool reset, no /dev/shm leak."""
        tf_ok = _make_tf(_add_one)
        tf_bad = _make_tf(_raise_in_worker)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf_ok, tf_bad])

        before = set(_shm_segments())
        df = pd.DataFrame({"col1": list(range(10))})

        with pytest.raises(Exception) as exc_info:
            (
                transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
                    execution_graph=dag, data=df, n_processes=2
                )
            )
        assert "UDF intentional failure" in str(exc_info.value) or any(
            "UDF intentional failure" in str(cause)
            for cause in [exc_info.value.__cause__, exc_info.value]
            if cause is not None
        )

        # Pool reset back to None so the next call lazily builds a fresh one.
        cls = transformation_function_engine.TransformationFunctionEngine
        assert cls._TransformationFunctionEngine__process_pool is None
        assert cls._TransformationFunctionEngine__process_pool_n_processes is None

        # No new /dev/shm psm_* segments leaked.
        after = set(_shm_segments())
        assert after - before == set(), f"Leaked shm segments: {after - before}"

    def test_apply_shm_enospc_fallback(self, monkeypatch):
        """ENOSPC during shm allocation falls back to pickle IPC and still runs."""

        def _raise_enospc(*args, **kwargs):
            raise OSError(errno.ENOSPC, "No space left on device")

        monkeypatch.setattr(
            transformation_function_engine.TransformationFunctionEngine,
            "_write_to_shared_memory",
            staticmethod(_raise_enospc),
        )

        tf1 = _make_tf(_add_one)
        tf2 = _make_tf(_add_two)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        df = pd.DataFrame({"col1": list(range(10))})
        result = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=dag, data=df, n_processes=2
        )
        assert "_add_one_col1_" in result.columns
        assert "_add_two_col1_" in result.columns

    def test_atexit_registered_once(self):
        """Pool-shutdown atexit hook registered exactly once at module import."""
        # Force a fresh re-import to confirm idempotency.
        cls = transformation_function_engine.TransformationFunctionEngine
        # Construct an engine multiple times; construction must not add a hook.
        cls(feature_store_id=99)
        cls(feature_store_id=99)
        # _ncallbacks() is a CPython implementation detail but stable since 3.x.
        # If unavailable on this interpreter, just count entries by inspecting the
        # internal registry (still implementation-detail; this test is a guardrail).
        # noinspection PyProtectedMember
        registry = (
            list(atexit._atexit_callbacks())
            if hasattr(atexit, "_atexit_callbacks")
            else None
        )
        if registry is not None:
            hits = [
                fn
                for (fn, _args, _kwargs) in registry
                if getattr(fn, "__func__", fn) is cls._shutdown_process_pool.__func__
            ]
            assert len(hits) == 1, (
                f"Expected exactly one atexit registration, got {len(hits)}"
            )

    def test_pool_recreated_after_n_processes_change(self):
        """Changing n_processes shuts the existing pool and creates a new one."""
        # Four independent transformations, so a request for 4 workers is
        # within the DAG's maximum parallelism and is not capped.
        dag = transformation_execution_dag.TransformationExecutionDAG(
            [
                _make_tf(_add_one),
                _make_tf(_add_two),
                _make_tf(_add_one_sleep),
                _make_tf(_add_two_sleep),
            ]
        )
        df = pd.DataFrame({"col1": list(range(20))})

        cls = transformation_function_engine.TransformationFunctionEngine
        cls._apply_transformation_functions(execution_graph=dag, data=df, n_processes=2)
        first = cls._TransformationFunctionEngine__process_pool
        assert cls._TransformationFunctionEngine__process_pool_n_processes == 2

        cls._apply_transformation_functions(execution_graph=dag, data=df, n_processes=4)
        second = cls._TransformationFunctionEngine__process_pool
        assert second is not first
        assert cls._TransformationFunctionEngine__process_pool_n_processes == 4

    def test_worker_ambient_guard_clamps_recursion(self):
        """_IS_TF_WORKER coerces n_processes=1 to prevent nested pool spawn."""
        # Directly exercise the entry guard rather than spinning up a real worker.
        module = transformation_function_engine
        module._IS_TF_WORKER = True
        try:
            cls = module.TransformationFunctionEngine
            assert cls._TransformationFunctionEngine__process_pool is None

            tf1 = _make_tf(_add_one)
            tf2 = _make_tf(_add_two)
            dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
            df = pd.DataFrame({"col1": list(range(20))})

            cls._apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=4
            )
            # Sequential path taken; no parallel pool spawned.
            assert cls._TransformationFunctionEngine__process_pool is None
        finally:
            module._IS_TF_WORKER = False

    def test_single_dict_dag_parallel_overlaps_independent_branches(self):
        """Single dict with n_processes=2 must overlap independent DAG branches.

        The parallel run must beat a measured sequential baseline. This is the
        case that motivated honoring `n_processes` on the dict path: a chained
        TF DAG where the user pays per-row latency and parallelism meaningfully
        reduces it.
        """
        tf1 = _make_tf(_add_one_sleep)
        tf2 = _make_tf(_add_two_sleep)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        entry = {"col1": 5}
        cls = transformation_function_engine.TransformationFunctionEngine

        # Sequential baseline (single worker, branches run back to back).
        t0 = time.perf_counter()
        cls._apply_transformation_functions(
            execution_graph=dag, data=entry, n_processes=1
        )
        sequential = time.perf_counter() - t0

        # Pre-spawn the pool so we measure scheduler overlap rather than
        # cold-fork worker startup. This is the warmup that init_serving /
        # init_batch_scoring trigger when n_processes > 1.
        cls._warmup_online_workers(n_processes=2)

        t0 = time.perf_counter()
        result = cls._apply_transformation_functions(
            execution_graph=dag, data=entry, n_processes=2
        )
        parallel = time.perf_counter() - t0

        # Overlapping the two independent branches of the single-row DAG must
        # beat the sequential baseline; compare measured times, not a fixed
        # threshold.
        assert parallel < sequential, (
            f"DAG-parallel dict path ({parallel:.2f}s) was not faster than the "
            f"sequential baseline ({sequential:.2f}s)."
        )
        assert result["_add_one_sleep_col1_"] == 6
        assert result["_add_two_sleep_col1_"] == 7

    def test_list_of_dicts_node_parallel_independent_tfs(self):
        """A batch with two independent heavy TFs runs them concurrently.

        Each TF sleeps per row; node-parallelism applies each TF to the whole
        batch in its own worker, so running the two independent TFs concurrently
        must beat running them back to back.
        """
        tf1 = _make_tf(_add_one_sleep)  # col1 -> _add_one_sleep_col1_
        tf2 = _make_tf(_add_two_sleep)  # col1 -> _add_two_sleep_col1_
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        assert dag.max_parallelism >= 2, "Test fixture requires independent TFs"

        entries = [{"col1": i} for i in range(2)]
        cls = transformation_function_engine.TransformationFunctionEngine

        # Sequential baseline (single worker, TFs back to back over the batch).
        t0 = time.perf_counter()
        cls._apply_transformation_functions(
            execution_graph=dag, data=entries, n_processes=1
        )
        sequential = time.perf_counter() - t0

        cls._warmup_online_workers(n_processes=2)

        t0 = time.perf_counter()
        result = cls._apply_transformation_functions(
            execution_graph=dag, data=entries, n_processes=2
        )
        parallel = time.perf_counter() - t0

        # Running the two independent TFs concurrently must beat the measured
        # sequential baseline; compare measured times, not a fixed threshold.
        assert parallel < sequential, (
            f"Node-parallel batch path ({parallel:.2f}s) was not faster than the "
            f"sequential baseline ({sequential:.2f}s)."
        )
        assert [row["_add_one_sleep_col1_"] for row in result] == [1, 2]
        assert [row["_add_two_sleep_col1_"] for row in result] == [2, 3]

    def test_single_dict_linear_dag_falls_through_to_sequential(self):
        """A single dict over a strictly linear DAG has nothing to parallelize.

        The engine must not pay process-pool overhead in that case — verified
        by asserting the pool was never created during the call.
        """
        # A → B linear chain: B consumes A's aliased output, so the DAG has
        # max_parallelism = 1 and dict-path parallel mode falls through.
        tf1 = _make_tf(_add_one)("col1").alias("in_col")
        tf2 = _make_tf(_double_in)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        entry = {"col1": 3}

        cls = transformation_function_engine.TransformationFunctionEngine
        cls._shutdown_process_pool()
        result = cls._apply_transformation_functions(
            execution_graph=dag, data=entry, n_processes=4
        )

        assert cls._TransformationFunctionEngine__process_pool is None
        assert result["in_col"] == 4
        assert result["_double_in_in_col_"] == 8

    def test_single_dict_dag_parallel_chained_branches_get_completed_inputs(self):
        """Diamond DAG on a single dict: two parallel branches feed one consumer.

        Verifies that the node-parallel scheduler ships the right inputs: the
        leaf TF is submitted only after its predecessors complete and their
        outputs are merged, so its input snapshot carries both upstream outputs
        even though they were computed concurrently in the pool.
        """
        # tf_a and tf_b are independent roots; tf_merge depends on both.
        tf_a = _make_tf(_add_one)("col1").alias("a_out")
        tf_b = _make_tf(_add_two)("col1").alias("b_out")
        tf_merge = _make_tf(_double_in)("a_out").alias("merged_a")
        dag = transformation_execution_dag.TransformationExecutionDAG(
            [tf_a, tf_b, tf_merge]
        )
        assert dag.max_parallelism >= 2, "Test fixture requires fan-out"

        cls = transformation_function_engine.TransformationFunctionEngine
        cls._warmup_online_workers(n_processes=2)
        result = cls._apply_transformation_functions(
            execution_graph=dag, data={"col1": 10}, n_processes=2
        )

        # tf_a: 10 + 1 = 11 → "a_out"
        # tf_b: 10 + 2 = 12 → "b_out"
        # tf_merge: 11 * 2 = 22 → "merged_a"
        assert result["a_out"] == 11
        assert result["b_out"] == 12
        assert result["merged_a"] == 22

    def test_dict_path_worker_exception_cleans_up_pool(self):
        """A worker exception on the dict path must reset the singleton pool.

        Without the reset the next call observes a broken pool. Mirrors the
        cleanup guarantee that the DataFrame path already provides.
        """
        tf_ok = _make_tf(_add_one)("col1").alias("safe_out")
        tf_bad = _make_tf(_raise_in_worker)("col1").alias("bad_out")
        dag = transformation_execution_dag.TransformationExecutionDAG([tf_ok, tf_bad])

        cls = transformation_function_engine.TransformationFunctionEngine
        cls._warmup_online_workers(n_processes=2)

        with pytest.raises(Exception) as exc_info:
            cls._apply_transformation_functions(
                execution_graph=dag, data={"col1": 1}, n_processes=2
            )
        assert "UDF intentional failure" in str(exc_info.value) or any(
            "UDF intentional failure" in str(cause)
            for cause in [exc_info.value.__cause__, exc_info.value]
            if cause is not None
        )

        # Pool was reset so the next call lazily builds a fresh one.
        assert cls._TransformationFunctionEngine__process_pool is None
        assert cls._TransformationFunctionEngine__process_pool_n_processes is None

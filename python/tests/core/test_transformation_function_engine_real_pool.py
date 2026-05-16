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
    time.sleep(0.2)
    return col1 + 1


@udf(int)
def _add_two_sleep(col1):
    time.sleep(0.2)
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
    hsfs_engine.set_instance(
        engine=python_engine_mod.Engine(), engine_type="python"
    )
    yield
    transformation_function_engine.TransformationFunctionEngine.shutdown_process_pool()


def _shm_segments() -> list[str]:
    if sys.platform.startswith("linux"):
        return glob.glob("/dev/shm/psm_*")
    return []


@pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow required for the shared-memory IPC path"
)
class TestRealPoolExecution:
    def test_apply_real_pool_parallel(self):
        """Two ready roots + a dependent. Real pool, two workers should overlap."""
        tf1 = _make_tf(_add_one_sleep)
        tf2 = _make_tf(_add_two_sleep)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])

        df = pd.DataFrame({"col1": list(range(10))})

        t0 = time.perf_counter()
        result = (
            transformation_function_engine.TransformationFunctionEngine
            .apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=2
            )
        )
        elapsed = time.perf_counter() - t0

        # Wall time strictly less than sequential (2 × 0.2s sleep).
        # 0.3s leaves headroom for spawn cost while still catching no-overlap regressions.
        assert elapsed < 0.35, (
            f"Parallel execution took {elapsed:.2f}s — expected <0.35s "
            "with overlap; sequential would be ~0.4s."
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

        parallel = (
            transformation_function_engine.TransformationFunctionEngine
            .apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=2
            )
        )
        sequential = (
            transformation_function_engine.TransformationFunctionEngine
            .apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=1
            )
        )
        assert parallel.to_dict(as_series=False) == sequential.to_dict(as_series=False)

    def test_apply_worker_exception_cleans_up(self):
        """UDF exception propagates with __cause__, pool reset, no /dev/shm leak."""
        tf_ok = _make_tf(_add_one)
        tf_bad = _make_tf(_raise_in_worker)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf_ok, tf_bad])

        before = set(_shm_segments())
        df = pd.DataFrame({"col1": list(range(10))})

        with pytest.raises(Exception) as exc_info:
            (
                transformation_function_engine.TransformationFunctionEngine
                .apply_transformation_functions(
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
        result = (
            transformation_function_engine.TransformationFunctionEngine
            .apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=2
            )
        )
        assert "_add_one_col1_" in result.columns
        assert "_add_two_col1_" in result.columns

    def test_atexit_registered_once(self):
        """Pool-shutdown atexit hook registered exactly once at module import."""
        # Force a fresh re-import to confirm idempotency.
        cls = transformation_function_engine.TransformationFunctionEngine
        # Construct an engine multiple times; previously each __init__ added a hook.
        cls(feature_store_id=99)
        cls(feature_store_id=99)
        # _ncallbacks() is a CPython implementation detail but stable since 3.x.
        # If unavailable on this interpreter, just count entries by inspecting the
        # internal registry (still implementation-detail; this test is a guardrail).
        # noinspection PyProtectedMember
        registry = list(atexit._atexit_callbacks()) if hasattr(atexit, "_atexit_callbacks") else None
        if registry is not None:
            hits = [
                fn
                for (fn, _args, _kwargs) in registry
                if getattr(fn, "__func__", fn) is cls.shutdown_process_pool.__func__
            ]
            assert len(hits) == 1, f"Expected exactly one atexit registration, got {len(hits)}"

    def test_pool_recreated_after_n_processes_change(self):
        """Changing n_processes shuts the existing pool and creates a new one."""
        tf = _make_tf(_add_one)
        dag = transformation_execution_dag.TransformationExecutionDAG([tf, _make_tf(_add_two)])
        df = pd.DataFrame({"col1": list(range(20))})

        cls = transformation_function_engine.TransformationFunctionEngine
        cls.apply_transformation_functions(execution_graph=dag, data=df, n_processes=2)
        first = cls._TransformationFunctionEngine__process_pool
        assert cls._TransformationFunctionEngine__process_pool_n_processes == 2

        cls.apply_transformation_functions(execution_graph=dag, data=df, n_processes=4)
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

            cls.apply_transformation_functions(
                execution_graph=dag, data=df, n_processes=4
            )
            # Sequential path taken; no parallel pool spawned.
            assert cls._TransformationFunctionEngine__process_pool is None
        finally:
            module._IS_TF_WORKER = False

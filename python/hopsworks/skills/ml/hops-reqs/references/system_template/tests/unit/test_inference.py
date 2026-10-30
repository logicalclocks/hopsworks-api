# ruff: noqa: INP001
"""The inference pipeline's output contract and the benchmark's arithmetic, offline."""

from __future__ import annotations

import importlib.util
import itertools
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from slug_pkg import inference_pipeline


def _benchmark():
    path = Path(__file__).resolve().parents[2] / "benchmarks" / "benchmark_inference.py"
    spec = importlib.util.spec_from_file_location("benchmark_inference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_prediction_rows_write_only_the_allowed_fields():
    keys = pd.DataFrame({"customer_id": [7, 8]})
    rows = inference_pipeline.prediction_rows(
        keys,
        [0.2, 0.9],
        datetime(2026, 9, 1, tzinfo=UTC),
        ["customer_id", "score"],
    )
    assert list(rows.columns) == ["customer_id", "score", "predicted_at"]
    assert list(rows["score"]) == [0.2, 0.9]


def test_percentiles_use_the_nearest_rank():
    bench = _benchmark()
    values = list(range(1, 101))
    assert bench.percentile(values, 50) == 50
    assert bench.percentile(values, 99) == 99


def test_a_failed_request_counts_as_an_error_at_the_timeout():
    bench = _benchmark()
    ticks = itertools.count()

    def clock():
        return next(ticks) * 0.01

    def send(key):
        if key == "bad":
            raise RuntimeError("boom")

    summary = bench.closed_loop(
        send,
        ["ok", "bad"],
        offered_qps=1000,
        concurrency=1,
        duration_s=0.5,
        warmup_s=0.0,
        timeout_ms=500,
        clock=clock,
    )
    assert summary["errors"] > 0
    assert summary["p99_ms"] == 500


def test_a_system_with_benchmark_requests_is_loaded_in_its_own_request_shape(monkeypatch):
    import sys
    import types

    bench = _benchmark()
    sent = []

    class Deployment:
        def predict(self, inputs):
            sent.append(inputs)

        def to_dict(self):
            return {"name": "fraud"}

    project = types.SimpleNamespace(
        get_model_serving=lambda: types.SimpleNamespace(get_deployment=lambda name: Deployment())
    )
    monkeypatch.setitem(sys.modules, "hopsworks", types.SimpleNamespace(login=lambda: project))
    pkg = inference_pipeline.__name__.split(".")[0]
    shaped = types.ModuleType(f"{pkg}.benchmark_requests")
    shaped.requests = lambda project, n: [[["card-1", 42.5]]] * n
    shaped.__spec__ = importlib.util.spec_from_loader(shaped.__name__, loader=None)
    monkeypatch.setitem(sys.modules, shaped.__name__, shaped)

    def closed_loop(send, requests, **kwargs):
        send(requests[0])
        return bench.summarize([10.0], 0, 1.0, 1.0)

    monkeypatch.setattr(bench, "closed_loop", closed_loop)
    system = {
        "system": {"slug": pkg.replace("_", "-")},
        "inference": {"realtime": {"deployment": "fraud", "model_version": 1}},
        "requirements": {"sla": {"realtime": {"p99_ms": 300, "throughput_qps": 1}}},
    }
    protocol = {
        "offered_qps": 1,
        "concurrency": 1,
        "duration_s": 1,
        "warmup_s": 0,
        "timeout_ms": 500,
    }
    bench.run_realtime(system, protocol, {"entities": 3})
    assert sent == [[["card-1", 42.5]]]


def test_batch_budget_runs_from_the_cron_fire_to_the_deadline():
    budget = inference_pipeline.batch_budget_s
    assert budget("0 0 4 1 * ?", "06:00 UTC") == 7200
    assert budget("0 0 4 1 * ?", "06:00 UTC", features_s=41) == 7159
    assert budget("0 */5 * * * ?", "06:00 UTC") is None


def test_only_a_full_window_that_fits_the_budget_is_met():
    system = {
        "requirements": {"sla": {"batch": {"must_finish_by": "06:00 UTC"}}},
        "inference": {"batch": {"job": {"schedule": {"cron": "0 0 4 1 * ?"}}}},
        "features": {"pipelines": [{"benchmark": {"last_run": {"full_window_s": 200}}}]},
    }
    verdict = inference_pipeline.batch_verdict(system, 4486, 147.0)
    assert verdict["budget_s"] == 7000 and verdict["met"] is True
    assert verdict["rows_per_s"] == 30.5
    assert inference_pipeline.batch_verdict(system, 10, 7001.0)["met"] is False
    assert inference_pipeline.batch_verdict(system, 10, 1.0, kind="sample")["met"] is False


def test_the_predictor_serves_scores_in_the_model_input_order():
    import numpy as np

    from slug_pkg.predictor import Predict

    class Classifier:
        def predict_proba(self, x):
            assert list(x.columns) == ["tenure_months"]
            return np.column_stack([1 - x["tenure_months"] / 100, x["tenure_months"] / 100])

    predictor = Predict.__new__(Predict)
    predictor.model = Classifier()
    predictor.model_input_columns = ["tenure_months"]
    vectors = pd.DataFrame({"customer_id": [7, 8], "tenure_months": [20, 50]})
    assert predictor.model_predict(vectors) == [0.2, 0.5]

# ruff: noqa: INP001
"""The inference pipeline's output contract and the benchmark's arithmetic, offline."""

from __future__ import annotations

import importlib.util
import itertools
from datetime import datetime, timezone
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
        datetime(2026, 9, 1, tzinfo=timezone.utc),
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


def test_batch_budget_runs_from_the_cron_fire_to_the_deadline():
    bench = _benchmark()
    assert bench.batch_budget_s("0 0 4 1 * ?", "06:00 UTC") == 7200
    assert bench.batch_budget_s("0 0 4 1 * ?", "06:00 UTC", features_s=41) == 7159
    assert bench.batch_budget_s("0 */5 * * * ?", "06:00 UTC") is None


def test_the_predictor_serves_scores_in_the_model_input_order():
    import numpy as np
    from slug_pkg.predictor import Predict

    class Classifier:
        def predict_proba(self, x):
            assert list(x.columns) == ["tenure_months"]
            return np.column_stack(
                [1 - x["tenure_months"] / 100, x["tenure_months"] / 100]
            )

    predictor = Predict.__new__(Predict)
    predictor.model = Classifier()
    predictor.model_input_columns = ["tenure_months"]
    vectors = pd.DataFrame({"customer_id": [7, 8], "tenure_months": [20, 50]})
    assert predictor.model_predict(vectors) == [0.2, 0.5]

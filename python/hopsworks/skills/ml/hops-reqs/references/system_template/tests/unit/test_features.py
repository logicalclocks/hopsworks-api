# ruff: noqa: INP001
"""The feature pipeline's window and validation, offline.

Add one test per transformation in feature_pipeline.py, on fixture rows with
the expected output beside them (the feature row of hops-reqs/references/tests.md).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from slug_pkg import feature_pipeline


def test_window_reads_the_scheduler_variables(monkeypatch):
    monkeypatch.setenv("HOPS_START_TIME", "2026-08-01T00:00:00Z")
    monkeypatch.setenv("HOPS_END_TIME", "2026-09-01T00:00:00Z")
    begin, end = feature_pipeline.window()
    assert begin == datetime(2026, 8, 1, tzinfo=UTC)
    assert end == datetime(2026, 9, 1, tzinfo=UTC)


def test_a_scheduled_run_ignores_the_fire_instant_and_reads_the_window(monkeypatch):
    # The scheduler appends -start_time with the instant it fired, which ends the window.
    monkeypatch.setenv("HOPS_START_TIME", "2026-09-28T01:00:00Z")
    monkeypatch.setenv("HOPS_END_TIME", "2026-10-05T01:00:00Z")
    system = {"features": {"pipelines": [{"name": feature_pipeline.PIPELINE}]}}
    monkeypatch.setattr(feature_pipeline, "_load_bundle", lambda b: (None, {}, system))
    seen = []

    def window(start=None, end=None):
        seen.append(feature_pipeline.__dict__["_window"](start, end))
        raise KeyboardInterrupt

    monkeypatch.setitem(feature_pipeline.__dict__, "_window", feature_pipeline.window)
    monkeypatch.setattr(feature_pipeline, "window", window)
    with pytest.raises(KeyboardInterrupt):
        feature_pipeline.main(["--bundle", "b.zip", "-start_time", "2026-10-05T01:00:00Z"])
    assert seen == [(datetime(2026, 9, 28, 1, tzinfo=UTC), datetime(2026, 10, 5, 1, tzinfo=UTC))]


def test_a_mistyped_option_still_fails():
    with pytest.raises(SystemExit):
        feature_pipeline.main(["--bundle", "b.zip", "--strat", "2026-10-05"])


def test_window_refuses_an_empty_interval():
    with pytest.raises(SystemExit):
        feature_pipeline.window("2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z")


def test_validation_rejects_null_and_duplicate_keys():
    good = pd.DataFrame({"customer_id": [1, 2], "x": [0.1, 0.2]})
    bad = pd.DataFrame({"customer_id": [1, 1, None], "x": [0.1, 0.2, 0.3]})
    assert feature_pipeline.validate(good, ["customer_id"]) == []
    assert len(feature_pipeline.validate(bad, ["customer_id"])) == 2


def _features_benchmark():
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "benchmarks" / "benchmark_features.py"
    spec = importlib.util.spec_from_file_location("benchmark_features", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_feature_window_must_land_before_inference_fires():
    bench = _features_benchmark()
    gap = bench.cron_gap_s("0 0 2 1 * ?", "0 0 4 1 * ?")
    assert gap == 7200
    assert bench.fits(41, gap)
    assert not bench.fits(5000, gap)
    assert bench.cron_gap_s("0 0 4 1 * ?", "0 0 2 1 * ?") is None
    assert bench.cron_gap_s("0 */5 * * * ?", "0 0 4 1 * ?") is None

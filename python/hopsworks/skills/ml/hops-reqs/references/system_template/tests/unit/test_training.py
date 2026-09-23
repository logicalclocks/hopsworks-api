# ruff: noqa: INP001
"""The split, the metrics and the registration rule, offline.

The training row of hops-reqs/references/tests.md. Extend it when the harness
changes before it is frozen; the loop never edits evaluate.py afterwards.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from slug_pkg import evaluate, training_pipeline


def test_time_split_is_ordered_and_leaves_out_immature_labels():
    b = evaluate.split_boundaries(
        datetime(2025, 1, 1), datetime(2026, 1, 1), 0.15, 0.15, timedelta(days=30)
    )
    assert b["train_start"] < b["train_end"] == b["validation_start"]
    assert (
        b["validation_start"] < b["validation_end"] == b["test_start"] < b["test_end"]
    )
    assert (
        b["test_end"] == b["label_cutoff"] == datetime(2026, 1, 1) - timedelta(days=30)
    )


def test_split_refuses_fractions_that_leave_no_train_part():
    with pytest.raises(ValueError):
        evaluate.split_boundaries(
            datetime(2025, 1, 1), datetime(2026, 1, 1), 0.5, 0.5, timedelta(0)
        )


def test_grouped_split_keeps_every_entity_in_one_part():
    parts = {e: evaluate.grouped_part(e, 0.15, 0.15) for e in range(5000)}
    assert parts == {e: evaluate.grouped_part(e, 0.15, 0.15) for e in range(5000)}
    shares = {
        p: sum(1 for v in parts.values() if v == p) / 5000 for p in evaluate.PARTS
    }
    assert abs(shares["test"] - 0.15) < 0.03
    assert abs(shares["validation"] - 0.15) < 0.03


def test_metrics_match_hand_computed_values():
    # positives at ranks 1 and 3: (1/1 + 2/3) / 2
    assert evaluate.pr_auc([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(5 / 6)
    # 3 of the 4 positive/negative pairs are ordered correctly
    assert evaluate.roc_auc([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == pytest.approx(0.75)
    assert evaluate.score("precision_at_2", [1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) == 0.5
    assert evaluate.rmse([1.0, 3.0], [2.0, 1.0]) == pytest.approx((2.5) ** 0.5)


def test_meets_follows_the_target_direction(system):
    requirements = system["requirements"]
    target = requirements["targets"]["target"]
    better = (
        target + 0.01
        if requirements["targets"]["direction"] == "max"
        else target - 0.01
    )
    assert evaluate.meets(requirements, better)


def test_feature_list_excludes_label_keys_event_time_and_leaky_names(system):
    problem = system["requirements"]["problem"]
    columns = [
        problem["target"],
        problem["entity"],
        "tenure_months",
        "resolved_at",
        "x",
    ]
    kept = training_pipeline.model_features(columns, system, leakage=["x"])
    assert kept == ["tenure_months"]


def test_research_always_registers_and_accept_only_when_met(system):
    assert training_pipeline.should_register("research", met=False)
    assert not training_pipeline.should_register("accept", met=False)
    assert not training_pipeline.should_register("retrain", met=False)
    assert training_pipeline.should_register("accept", met=True)
    assert training_pipeline.model_name("research", system).endswith("_research")
    assert training_pipeline.model_name("retrain", system).endswith("_model")

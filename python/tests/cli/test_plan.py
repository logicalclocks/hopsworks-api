"""`hops factory system plan`: a phase's state read from what the project holds."""

from __future__ import annotations

import pytest
import yaml
from hopsworks.cli import plan, system_doc


SPEC = {
    "schema_version": 1,
    "factory": {
        "name": "ml-batch",
        "phases": [
            {"key": k}
            for k in (
                "requirements",
                "data",
                "features",
                "training",
                "inference",
                "app",
                "verify",
            )
        ],
    },
    "system": {"slug": "churn", "progress": {"phase": "data", "done": ["reqs"]}},
    "requirements": {"status": "met"},
    "data": {"status": "pending"},
    "features": {
        "status": "pending",
        "pipelines": [
            {
                "writes": {"feature_group": "churn_customers", "version": 1},
                "job": {"name": "churn-features"},
            }
        ],
    },
    "training": {
        "status": "skipped",
        "reason": "the user brought churn_model v3",
        "feature_view": {"name": "churn_fv", "version": 1},
        "model": {"name": "churn_model", "version": 3},
        "runs": [{"model": {"name": "churn_research", "version": 1}}],
    },
    "inference": {
        "status": "pending",
        "batch": {
            "reads": {"feature_view": "churn_fv"},
            "job": {"name": "churn-inference"},
        },
        "model": "churn_model v3",
    },
    "app": {"status": "pending", "name": "churn-app"},
    "verify": {"status": "pending"},
}


def _exists(present: set[str], unknown: set[str] = frozenset()):
    def exists(ref: plan.Ref):
        if str(ref) in unknown:
            return None
        # A ref without a version is any version, as the lookup reads it.
        return str(ref) in present or (
            ref.version is None and any(p.startswith(f"{ref} v") for p in present)
        )

    return exists


def _states(result: dict) -> dict:
    return {p["phase"]: p["state"] for p in result["phases"]}


def test_a_brought_model_starts_the_build_at_inference():
    result = plan.compute(
        SPEC, _exists({"feature view churn_fv v1", "model churn_model v3"})
    )
    assert _states(result) == {
        "requirements": "done",
        "data": "todo",
        "features": "build",
        "training": "done",
        "inference": "build",
        "app": "build",
        "verify": "todo",
    }
    inference = next(p for p in result["phases"] if p["phase"] == "inference")
    assert inference["makes"] == ["job churn-inference"]
    assert inference["needs"] == ["feature view churn_fv", "model churn_model v3"]
    # The research runs are history: their models are not what training makes.
    training = next(p for p in result["phases"] if p["phase"] == "training")
    assert "model churn_research v1" not in training["makes"]


def test_a_goal_plans_only_what_it_needs():
    result = plan.compute(
        SPEC,
        _exists({"feature view churn_fv v1", "model churn_model v3"}),
        goal="inference",
    )
    assert _states(result) == {
        "requirements": "done",
        "training": "done",
        "inference": "build",
    }
    assert result["next"] == "inference"
    with pytest.raises(ValueError):
        plan.compute(SPEC, _exists(set()), goal="nope")


def test_a_deleted_asset_takes_a_met_phase_back_and_a_lookup_error_decides_nothing():
    spec = {**SPEC, "training": {**SPEC["training"], "status": "met"}}
    assert (
        _states(plan.compute(spec, _exists({"feature view churn_fv v1"})))["training"]
        == "build"
    )
    unsure = plan.compute(
        spec, _exists({"feature view churn_fv v1"}, {"model churn_model v3"})
    )
    assert _states(unsure)["training"] == "unknown"


def test_a_phase_that_has_not_finished_is_not_done_by_what_it_recorded_so_far():
    spec = {
        **SPEC,
        "training": {
            "status": "running",
            "feature_view": {"name": "churn_fv", "version": 1},
        },
    }
    assert (
        _states(plan.compute(spec, _exists({"feature view churn_fv v1"})))["training"]
        == "build"
    )


def test_a_judgement_on_existing_assets_stands():
    spec = {**SPEC, "training": {**SPEC["training"], "status": "unmet"}}
    result = plan.compute(
        spec, _exists({"feature view churn_fv v1", "model churn_model v3"})
    )
    assert _states(result)["training"] == "failed"


def test_sync_writes_met_stale_and_progress(tmp_path):
    spec = {**SPEC, "features": {**SPEC["features"], "status": "met"}}
    system_doc.create(tmp_path, spec)
    present = {"feature view churn_fv v1", "model churn_model v3"}
    plan.sync(tmp_path, plan.compute(spec, _exists(present)))
    doc = yaml.safe_load((tmp_path / "system.yaml").read_text())
    assert doc["training"]["status"] == "skipped"
    assert doc["features"]["status"] == "stale"
    assert doc["requirements"]["status"] == "met"
    assert doc["system"]["progress"]["done"] == ["reqs", "train"]
    assert doc["system"]["progress"]["phase"] == "data"


def test_sync_refuses_another_invocations_lease(tmp_path, monkeypatch):
    system_doc.create(tmp_path, SPEC)
    system_doc.acquire(tmp_path)
    monkeypatch.setenv(system_doc.TOKEN_ENV, "not-the-token")
    with pytest.raises(system_doc.LeaseHeld):
        plan.sync(tmp_path, plan.compute(SPEC, _exists(set())))


def test_an_analytics_layer_keeps_its_phases_and_lists_gone_outputs():
    layer = {
        "layer": {"kind": "bronze", "slug": "crm"},
        "phases": {
            "code": {"status": "done"},
            "backfill": {"status": "done"},
            "verify": {"status": "pending"},
        },
        "outputs": {
            "tables": [{"name": "crm_deals", "version": 1}],
            "jobs": [{"name": "crm-ingest"}],
        },
    }
    result = plan.compute(layer, _exists({"job crm-ingest"}))
    assert _states(result) == {"code": "done", "backfill": "done", "verify": "todo"}
    assert result["outputs_missing"] == ["feature group crm_deals v1"]
    assert result["next"] == "verify"


def test_a_gold_layer_lists_its_marts_gone_tables():
    gold = {
        "layer": {"kind": "gold", "slug": "sales"},
        "marts": [
            {
                "name": "revenue",
                "phases": {"code": {"status": "done"}},
                "tables": [{"name": "fct_orders", "version": 1}],
            }
        ],
    }
    result = plan.compute(gold, _exists(set()))
    assert result["phases"] == []
    assert result["outputs_missing"] == ["feature group fct_orders v1"]

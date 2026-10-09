# ruff: noqa: INP001
"""Training/serving skew: the batch path and the online path agree on the same entities.

Two checks at one moment, with the model and transformation versions the YAML
pins. First, the transformed features the offline store gives for each entity
at `parity.at` match the transformed vector the online store serves. Second,
the downloaded model scoring those vectors matches what the deployment returns
for the same keys, one prediction per entity. `<pkg>.parity` holds the comparison and its null
policy: null on both sides is equal, null on one side or an infinity is a mismatch. Applies only
where both paths exist (a realtime system).
"""

from __future__ import annotations

import importlib

import pytest


def _parity(system: dict) -> dict:
    parity = ((system.get("inference") or {}).get("tests") or {}).get("parity")
    if not parity or "realtime" not in system["inference"]:
        pytest.fail("parity is declared only for a realtime system; remove this file otherwise")
    return parity


def test_batch_and_online_paths_agree(project, system):
    parity = _parity(system)
    entity = system["requirements"]["problem"]["entity"]
    training = system["training"]
    fs = project.get_feature_store()
    fv = fs.get_feature_view(
        training["feature_view"]["name"], version=training["feature_view"]["version"]
    )
    td_version = training["harness"]["training_dataset_version"]
    fv.init_serving(training_dataset_version=td_version)
    fv.init_batch_scoring(training_dataset_version=td_version)

    batch = fv.get_batch_data(end_time=parity["at"], primary_key=True, event_time=True)
    key_column = next(c for c in batch.columns if c == entity or c.endswith("_" + entity))
    event_times = {
        s["event_time"] for s in system["requirements"]["data_sources"] if s.get("event_time")
    }
    time_column = next(
        c for c in batch.columns if any(c == t or c.endswith("_" + t) for t in event_times)
    )
    latest = batch.sort_values(time_column).groupby(key_column).tail(1)
    sample = latest.sample(min(parity["entities"], len(latest)), random_state=0)
    keys = list(sample[key_column])

    online = fv.get_feature_vectors(entry=[{entity: k} for k in keys], return_type="pandas")
    offline = sample.drop(columns=[key_column, time_column])
    # The feature view's schema, so a column missing on either side is a mismatch, not skipped.
    expected = [
        f.name for f in fv.features if not f.label and f.name not in (key_column, time_column)
    ]
    pkg = system["system"]["slug"].replace("-", "_")
    parity_check = importlib.import_module(f"{pkg}.parity")
    problems = parity_check.feature_problems(
        offline, online, expected, len(keys), parity["tolerance"], parity["max_mismatch"]
    )
    assert not problems, "; ".join(problems)

    evaluate = importlib.import_module(f"{pkg}.evaluate")
    model = project.get_model_registry().get_model(
        training["model"]["name"], version=parity["model_version"]
    )
    local = evaluate.load_predictor(model.download())(online)
    deployment = project.get_model_serving().get_deployment(
        system["inference"]["realtime"]["deployment"]
    )
    served = [deployment.predict(inputs=[{entity: k}])["predictions"][0] for k in keys]
    problems = parity_check.prediction_problems(
        local, served, len(keys), parity["tolerance"], parity["max_mismatch"]
    )
    assert not problems, "; ".join(problems)

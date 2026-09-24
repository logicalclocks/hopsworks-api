# Tests, benchmarks and the shared harness

A pipeline phase is `met` only when its tests exist, pass, and are recorded in
its `tests.last_run` with the run id and commit they tested. Anything that needs
a running system is an integration test, never a unit test behind a skip guard.

## Layout

The [system template](system_template/) carries the harness; copy it once per system.

| File | What it is |
| --- | --- |
| `pyproject.toml` | `pythonpath = ["src"]`, `testpaths = ["tests/unit"]`: plain `pytest` runs only the unit tests |
| `tests/conftest.py` | `system` (the YAML), `load_fixture`, `project` (fails without a connection), `test_objects` (deleted after each test, on failure too), `SUFFIX` (`_test_<run_id>`) |
| `tests/unit/test_system_yaml.py` | the rules of system-yaml.md checked on this system's file; also a CLI for the atomic write |
| `tests/integration/conftest.py` | an integration run that collects zero tests fails |
| `tests/integration/test_parity.py` | the training/serving skew check, realtime systems only |
| `tests/run_integration.py` | the `<slug>-tests` job: fetches the bundle, runs pytest in the cluster, writes result.json |
| `benchmarks/benchmark_inference.py` | the SLA as a program; exit code 0 when it holds |

```bash
pytest                                  # unit tests, offline, seconds
pytest tests/integration                # from a connected terminal; fails, never skips, without one
python <slug>/bundle.py make tests-features-1 --with-tests
hops files upload <slug>/runs/tests-features-1/bundle.tar.gz Resources/<slug>/runs/tests-features-1/
hops job deploy <slug>-tests <slug>/tests/run_integration.py --env <environment> \
  --args "--bundle Resources/<slug>/runs/tests-features-1/bundle.tar.gz tests/integration/test_feature_pipeline.py" --run --wait --overwrite
```

The tests job runs in the environment of the pipeline under test when it has
`pytest` (every base built on `python-feature-pipeline` does). Otherwise it runs
in `<env>-tests`, a clone of that exact environment with only `pytest` added:
one per environment the system uses, never one shared clone.

## What each pipeline's tests check

| Pipeline | Unit (`tests/unit/`) | Integration (`tests/integration/`) |
| --- | --- | --- |
| data (synthetic only) | a fixed seed gives the same frame twice; columns and types match the declared schema; the target's base rate is within tolerance of `targets.prevalence`; the story holds on a sample (signal columns correlate with the target in the declared direction); no column reproduces the target; `--mode live` for one tick yields `rate_per_s x tick_s` rows inside the tick | `--mode backfill` over one day into `<ident>_test_<run_id>` lands the expected count with unique keys; for events, three live ticks into an online test group land at least half the expected rows, readable online; the groups are deleted |
| feature | each transformation on fixture rows gives the expected output, nulls included; idempotence where it should hold; output columns and types match the sink's features (a contract test); validation rejects a crafted bad row | the pipeline over the fixture sample or a bounded window into a test group lands the expected count, unique keys, no nulls in non-nullable features; a second run adds no rows; the group is deleted |
| training | `evaluate.py`: parts disjoint; time split ordered (`max(train) < min(validation) < min(test)`) and rows younger than `label_maturity` excluded; grouped split has no entity in two parts; each metric matches a hand-computed value. `training_pipeline.py`: the feature list excludes the label, identifiers, event time and suspicious names; `research` registers under `<ident>_research` whatever the metric; `accept` and `retrain` register under `<ident>_model` only on a passing test metric | `accept` on a small slice registers `<ident>_model_test_<run_id>` with metrics and provenance when the threshold holds, and nothing with an impossible threshold; transformations at training read match a batch read of the same rows; the test model is deleted |
| inference | request to feature vector assembly yields the model's columns in order; missing and extra fields handled as designed; response schema; for batch, the window follows the cadence; `prediction_rows` writes only `log_fields` | batch: a small window scored into `<ident>_predictions_test_<run_id>` has the expected count, schema and range; realtime: the benchmark at a short duration returns 200s and the schema; **parity** (`test_parity.py`) for realtime systems |

Unit tests import the pipeline functions directly (`from telco_churn.evaluate import split_boundaries`),
which `pythonpath = ["src"]` allows without installing anything.

```python
# tests/unit/test_training.py
from datetime import datetime, timedelta

from telco_churn import evaluate, training_pipeline


def test_time_split_is_ordered_and_leaves_out_immature_labels():
    b = evaluate.split_boundaries(datetime(2025, 1, 1), datetime(2026, 1, 1), 0.15, 0.15, timedelta(days=30))
    assert b["train_end"] <= b["validation_start"] < b["validation_end"] <= b["test_start"] < b["test_end"]
    assert b["test_end"] == datetime(2026, 1, 1) - timedelta(days=30)


def test_pr_auc_matches_a_hand_computed_value():
    # positives at ranks 1 and 3: (1/1 + 2/3) / 2
    assert abs(evaluate.pr_auc([1, 0, 1, 0], [0.9, 0.8, 0.7, 0.1]) - 5 / 6) < 1e-12


def test_research_always_registers_and_accept_only_when_met(system):
    assert training_pipeline.should_register("research", met=False)
    assert not training_pipeline.should_register("accept", met=False)
    assert training_pipeline.model_name("accept", system).endswith("_model")
```

## Rules

- Tests never touch production objects. Everything a test creates carries
  `SUFFIX` and goes into `test_objects`. A resume deletes any `*_test_*` object
  of this system whose run is no longer running.
- Fixtures are a few hundred rows with the expected outputs beside them, under
  `tests/fixtures/`. By `requirements.data_policy.fixtures` they are generated
  from the source schema (the default), sanitised real rows, or real rows, which
  needs `export_ok: true`: a fixture is committed, and a sample in a private
  repository is still an export.
- A failing test makes the phase `unmet`; a changed entrypoint makes its
  recorded run `stale`. A test weakened to pass is a design change and gets a
  `decisions` line.
- In the autonomous loops unit tests run on every attempt; the integration
  suite runs on the attempt that would otherwise be declared `met`.

## The benchmark

`benchmarks/benchmark_inference.py` reads `requirements.sla` and `inference`,
follows `inference.mode`, and exits 0 when the SLA holds and 1 when it does not.

```yaml
inference:
  benchmark:
    file: benchmarks/benchmark_inference.py
    job: telco-churn-benchmark
    protocol: {load: closed_loop, offered_qps: 100, concurrency: 8, duration_s: 60, warmup_s: 10, timeout_ms: 500,
               min_requests: 5000, location: in_cluster_job, errors: counted, frozen_at: 9a1b2c3}
    params: {duration_s: 60, qps: 100, entities: 500}      # batch: {start: ..., end: ...}
```

- The protocol is frozen for a tuning round: an attempt changes the deployment,
  never how it is measured; a protocol change starts a new round with a new baseline.
- Above about 50 qps it runs as the `<slug>-benchmark` job, so the network path
  measured is the consumers' rather than a laptop's.
- A timed-out or failed request counts as an error and as the maximum latency; the first
  three failures are kept in the result as `error_samples`.
- A run that misses the SLA exits 1, so the `<slug>-benchmark` job shows FAILED: read the
  verdict from `result.json`, and never attach a failure alert to the benchmark job.
- `--record` writes result.json, imported as one `measured` line with the
  attempt's identity (commit, model version, deployment configuration hash).
  `--duration` is a one-off check, labelled `sample` and never recorded.
- Batch mode scores into a throwaway `<ident>_predictions_test_<run_id>` group
  it deletes, and `met` needs a `full_window` run whose time fits between the
  inference cron fire and `sla.batch.must_finish_by`, less the measured feature job.

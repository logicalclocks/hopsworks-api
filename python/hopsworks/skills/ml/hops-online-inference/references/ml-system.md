# Online inference inside an ML system (`/hops-build`)

Read with `hops skills info hops-online-inference references/ml-system.md` in the infer phase of an ML system.

## Tests

The inference row of [hops-reqs/references/tests.md](../../hops-reqs/references/tests.md) for realtime.
Unit, offline: request to feature vector assembly yields the model's columns in
order, missing and extra fields are handled as designed, and the response has
the declared schema. Integration: the benchmark at a short duration returns 200s
and the schema, and **parity** (`tests/integration/test_parity.py` in the
template): for a sample of entities at `tests.parity.at`, the offline and online
stores give the same transformed features, and the downloaded model scoring
those vectors matches the deployment's predictions, within
`tests.parity.tolerance` (default `1e-6`, `max_mismatch: 0`). That is the
training/serving skew check, and it is why the inference environment must hold
`training.environment.transformation_libraries` at exactly those versions.

Inside a `/hops-build` system the deployment is the registered model with the
template's `src/<slug_pkg>/predictor.py`, a `DefaultPredict` subclass that keeps
the default feature lookup, transformations and schema validation but returns
the positive-class probability for a classifier, the number `evaluate.py` and
the batch path compute; the default returns class labels, and parity would fail
on every entity:

```python
model.deploy(name="telcochurnrt", script_file="src/telco_churn/predictor.py", default_predictor=True)
```

The predictor container defaults to a 1 GiB limit, which the default predictor
outgrows even for a small scikit-learn model: an `OOMKilled` restart shows up
as 502 `connection refused` from `predict`. Raise
`deployment.resources.limits.memory` within `budget.inference.max_memory_mb`,
`deployment.save(await_update=...)`, and measure again.

The SLA benchmark, its load protocol and its pass rule: [references/benchmark.md](references/benchmark.md).

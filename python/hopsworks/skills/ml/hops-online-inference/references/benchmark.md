# Benchmark contract

How the inference SLA is measured in a `/hops-build` system.
The parent skill's "Tests" section covers the unit and integration tests; this file is the benchmark they and `/hops verify benchmark` run.

`benchmarks/benchmark_inference.py` is how the SLA is measured, by the inference agent on every attempt, by the integration test at a short duration, and by `/hops verify benchmark` at full length:

- Closed-loop load at `protocol.offered_qps` with a fixed `concurrency`, after `warmup_s`, for `duration_s` and at least `min_requests`, every request with `timeout_ms`, using real entity keys from the feature store.
- A timed-out or failed request counts as an error and as the maximum latency, never dropped; the first three failures are kept as `error_samples` so an unmet SLA says why.
- Reports p50, p95 and p99 against `sla.realtime.p99_ms`, offered and completed qps against `throughput_qps`, and the error rate against `error_rate_max`; exits 0 when all three hold and 1 otherwise.
- Above about 50 qps it runs as the `<slug>-benchmark` job, so the path measured is the consumers'.
- The protocol is frozen for a tuning round: an attempt changes the deployment (replicas, batched lookups, resources), never how it is measured.

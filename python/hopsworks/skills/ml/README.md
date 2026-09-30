# ML skills

Develop and operate ML systems with the FTI (feature / training / inference)
pipeline architecture: specify the system, build feature groups and views,
train, serve, and monitor. The live, canonical list is `hops skills list --bucket ml`.

- [hops-reqs](hops-reqs/SKILL.md) — Specify an ML system as ordered FTI pipelines; write the spec to `reqs/`.
- [hops-features](hops-features/SKILL.md) — Specify a feature pipeline; hands off to hops-fg / hops-fv.
- [hops-eda](hops-eda/SKILL.md) — Exploratory data analysis (bundled profiler scripts) before training.
- [hops-eda-checklist](hops-eda-checklist/SKILL.md) — Reference: the dimensions to cover in EDA (profiling, target, per-feature, leakage).
- [hops-fg](hops-fg/SKILL.md) — Create, insert into, read, and manage feature groups (Python SDK).
- [hops-fv](hops-fv/SKILL.md) — Create and query feature views, training data, online feature vectors.
- [hops-transformations](hops-transformations/SKILL.md) — Built-in / custom `@udf` / model-dependent / on-demand transforms and the transformation store.
- [hops-train](hops-train/SKILL.md) — Train a model from a feature view and register it.
- [hops-batch-inference](hops-batch-inference/SKILL.md) — Batch scoring; persist/log predictions.
- [hops-online-inference](hops-online-inference/SKILL.md) — Deploy a model (KServe) and serve online predictions.
- [hops-monitoring](hops-monitoring/SKILL.md) — Statistics, feature/drift monitoring, Great Expectations validation, alerts, feature logging.

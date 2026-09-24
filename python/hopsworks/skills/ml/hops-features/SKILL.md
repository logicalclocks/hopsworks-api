---
name: hops-features
description: Design, build, backfill and schedule a feature pipeline program (batch or streaming) that writes model-independent features to feature groups. Auto-invoke when the user wants a feature pipeline or new features computed from a data source; inside /hops-build it builds the features phase from system.yaml.
---

# Feature Pipeline

This skill should be invoked when the user wants to create a feature pipeline program.

A feature pipeline reads from data sources, applies model-independent transformations (MITs) to produce reusable features, and writes them to feature groups in the feature store. It is one of the three FTI (feature/training/inference) pipelines, developed and operated independently. MITs (aggregations, lagged/windowed features, binning, data validation/cleaning, vector embeddings) are the only transformations that belong here. Model-dependent transformations (encoding, scaling, normalization) belong in the training and inference pipelines, not the feature pipeline. Features written here are reusable across many models, so prefer reusing an existing feature over recomputing it.

## Contract
- **Input:** the ML-system requirements (inputs, new features to compute, freshness/SLAs, framework preferences, dependencies).
- **Output:** a feature pipeline program, its sink feature group(s), and its job (scheduled or continuous). Inside a `/hops-build` system the output is also one entry of `features.pipelines` in `system.yaml`: write that entry and preserve every other line ([hops-reqs/references/system-yaml.md](../hops-reqs/references/system-yaml.md)).
- **Pre-condition:** the ML-system requirements exist (see hops-reqs); data sources and feature groups are known.

## Where a feature is computed

`requirements.features[].computed_in` is one of:

- `feature_pipeline`: a reusable, model-independent feature, computed here in a scheduled batch job.
- `streaming`: the same, but freshness demands a continuous job (PySpark Structured Streaming).
- `on_demand`: needs a request-time parameter; not built here. It is attached to the feature view in training and executed at inference.

## Ask the user

Use AskUserQuestion (step 3) about every aspect of the plan until you reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. Confirm the proposed deep modules and which modules they want tests for (step 4).

## Steps

1. The ML system requirements step should provide the inputs, and the new features that will be computed as outputs, and feature freshness requirements, SLAs (uptime in number of nines), preferred processing frameworks (for batch: Polars, Pandas, Spark; for streaming: Spark Streaming), and any ideas for solutions. Check if the feature pipeline has dependencies on the outputs of other feature pipelines that should run before it and write down the dependencies.

2. Understand any existing source code in the repo and the available data sources and feature groups.

3. AskUserQuestion about every aspect of this plan until you reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. 

4. Sketch out the data sources, frameworks that will be used, and transformations that will be performed to build or modify to complete the implementation. Actively look for opportunities to extract deep modules that can be tested in isolation. A deep module (as opposed to a shallow module) is one which encapsulates a lot of functionality in a simple, testable interface which rarely changes. Check with the user that these modules match their expectations. Check with the user which modules they want tests written for.

5. Once you have a complete understanding of the problem and solution, write the pipeline (inside a system, from `src/<slug_pkg>/feature_pipeline.py` in the hops-reqs template: one program for backfill and increments, the window from `HOPS_START_TIME` and `HOPS_END_TIME`), run the backfill as a job, verify what landed from outside the program with bounded reads (`hops sql` for a count and the newest event time, `hops fg preview` for a sample), attach the schedule with its window offsets and failure alert, and record the pipeline. The engine follows estimated peak memory (rows x columns x bytes per value x 3, the **hops-fg** sizing rule) against the job's memory: pandas or polars in `python-feature-pipeline` when it fits with room, PySpark in `spark-feature-pipeline` when it does not or the pipeline is continuous. Both bases already hold what a pipeline needs, so a feature pipeline never clones an environment.

## What the pipeline design covers

### Data

The input data sources, the data that will be read, the transformations (MITs) that will be applied, and the sink feature group(s) for the data. Whether this a batch program or a streaming program. Will a transformation be used at runtime and require request-time parameters to be computed? That is an on-demand transformation (ODT); if yes, then create as a custom transformation in Hopsworks and attach it to the feature group. The same ODT function can be reused in the feature pipeline over historical data and in the online inference pipeline at request time, which avoids training/serving skew.

Make the pipeline safe to rerun: writes to feature groups should be idempotent and atomic so that a failure and retry does not leave duplicate or partial feature data. Validate feature data (e.g. with a Great Expectations expectation suite on the feature group) before it is written, so one bad record does not later break a training or inference run.

### Job
Is it a batch or streaming job? The same program should both backfill historical data and process new incremental data, parameterized by start_time/end_time over the data source's event_time. Do you need to first run a backfill job with start/end time for the data sources? Do the source feature groups or data sources support event_time?
Will it be a simple job execution or a scheduled job (optionally with incremental reads from feature groups and/or data sources). Load hops-job skill.

### Sink
One or more feature groups should be the sink of the feature pipelines. Use the **hops-fg** skill to create and write them (online vs offline, schema, provenance). For external/source data, use **hops-data-sources**; for PySpark processing, **hops-spark**. Set a `description=` on each sink feature group and on every `Feature(...)`: undescribed features land as empty envelopes in the UI and are not discoverable.

### Data processing framework
Which framework was chosen based on expected workload size, feature freshness requirements, and user preferences.

### Tests

The feature row of [hops-reqs/references/tests.md](../hops-reqs/references/tests.md). Unit tests, offline, in `tests/unit/test_features.py`: each transformation on fixture rows gives the expected output, nulls included; applying it twice equals once where it should; the output columns and types match the sink's declared features (a contract for downstream consumers); validation rejects a crafted bad row. An integration test, `tests/integration/test_feature_pipeline.py`, runs the pipeline on the fixture sample or a bounded window into `<ident>_test_<run_id>`, asserts the row count, unique keys and no nulls in non-nullable features, runs it again to show no duplicates, and deletes the test group. The pipeline is `met` only when both pass and `tests.last_run` records the run id and commit.

## Next Steps

- Implement the sink feature groups: **hops-fg**. Run on a schedule: **hops-job**.
- Then build the view and train: **hops-fv**, **hops-train**. Overall plan: **hops-reqs**.

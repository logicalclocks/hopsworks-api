---
name: hops-features
description: Create and schedule/run a feature pipeline program from additional user input. Build on the ML system requirements. Input: ML-system requirements; Output: a reqs/*-pipeline.md spec.
---

# Feature Pipeline

This skill should be invoked when the user wants to create a feature pipeline program.

## Contract
- **Input:** the ML-system requirements (inputs, new features to compute, freshness/SLAs, framework preferences, dependencies).
- **Output:** a feature-pipeline specification written as a local markdown file in `reqs/` (`reqs/feature-pipeline.md`, `reqs/training-pipeline.md`, or `reqs/inference-pipeline.md`).
- **Pre-condition:** the ML-system requirements exist (see hops-reqs); data sources and feature groups are known.

## Ask the user

Use AskUserQuestion (step 3) about every aspect of the plan until you reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. Confirm the proposed deep modules and which modules they want tests for (step 4).

## Steps

1. The ML system requirements step should provide the inputs, and the new features that will be computed as outputs, and feature freshness requirements, SLAs (uptime in number of nines), preferred processing frameworks (for batch: DuckDB, Polars, Pandas, Sparks; for streaming: Spark Streaming), and any ideas for solutions. Check if the feature pipeline has dependencies on the outputs of other feature pipelines that should run before it and write down the dependencies.

2. Understand any existing source code in the repo and the available data sources and feature groups.

3. AskUserQuestion about every aspect of this plan until you reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. 

4. Sketch out the data sources, frameworks that will be used, and transformations that will be performed to build or modify to complete the implementation. Actively look for opportunities to extract deep modules that can be tested in isolation. A deep module (as opposed to a shallow module) is one which encapsulates a lot of functionality in a simple, testable interface which rarely changes. Check with the user that these modules match their expectations. Check with the user which modules they want tests written for.

5. Once you have a complete understanding of the problem and solution, use the template below to write the specification. The reqs (pipeline requirements) should be written as a local markdown file at reqs/feature-pipeline.md, reqs/training-pipeline.md or reqs/inference-pipeline.md. Create the reqs/ directory if it doesn’t exist. Do NOT call any external service.

## Store the specification as well

The input data sources, the data that will be read, the transformations that will be applied, and the sink feature group(s) for the data. Whether this a batch program or a streaming program. Will a transformation be used at runtime and require request-time parameters to be computed? If yes, then create as a custom transformation in Hopsworks and attach it to the feature group.

## Job
Is it a batch or streamining job? Do you need to first run a backfill job with start/end time for the data sources? Do the source feature groups or data sources support event_time?
Will it be a simple job execution or a scheduled job (optionally with incremental reads from feature groups and/or data sources). Load hops-job skill.

## Sink
One or more feature groups should be the sink of the feature pipelines. Use the **hops-fg** skill to create and write them (online vs offline, schema, provenance). For external/source data, use **hops-data-sources**; for PySpark processing, **hops-spark**.

## Next Steps

- Implement the sink feature groups: **hops-fg**. Run on a schedule: **hops-job**.
- Then build the view and train: **hops-fv**, **hops-train**. Overall plan: **hops-reqs**.

# Data Processing Framework
Which framework was chosen based on expected workload size, feature freshness requirements, and user preferences.

# Testing Decisions

Can you save some sample input data that can be used to implement an integration test that reads the sample data, transforms it, writes it to a test feature group created when needed, read the data written, and then delete test feature group. Add unit test for transformations that should be contracts for downstream consumers of the engineered features.


---
name: hops-feature-pipeline
description: Create a requirements document for a feature pipeline and write it down as a markdown file in reqs/. Use when the user wants to turn the ML system requirements into a feature pipeline program requirements. 
---

This skill should be invoked when the user wants to create a feature pipeline program.

1. The ML system requirements step should provide the inputs, and the new features that will be computed as outputs, and feature freshness requirements, SLAs (uptime in number of nines), preferred processing frameworks (for batch: DuckDB, Polars, Pandas, Sparks; for streaming: Spark Streaming), and any ideas for solutions. Check if the feature pipeline has dependencies on the outputs of other feature pipelines that should run before it and write down the dependencies.

2. Understand any existing source code in the repo and the available data sources and feature groups.

3. Interview the user relentlessly about every aspect of this plan until you reach a shared understanding. Walk down each branch of the design tree, resolving dependencies between decisions one-by-one. 

4. Sketch out the data sources, frameworks that will be used, and transformations that will be performed to build or modify to complete the implementation. Actively look for opportunities to extract deep modules that can be tested in isolation. A deep module (as opposed to a shallow module) is one which encapsulates a lot of functionality in a simple, testable interface which rarely changes. Check with the user that these modules match their expectations. Check with the user which modules they want tests written for.

5. Once you have a complete understanding of the problem and solution, use the template below to write the specification. The reqs (pipeline requirements) should be written as a local markdown file at reqs/feature-pipeline.md, reqs/training-pipeline.md or reqs/inference-pipeline.md. Create the reqs/ directory if it doesn’t exist. Do NOT call any external service.

<feature-pipeline-reqs>

## Data Sources, Sinks, and Transformations

The input data sources, the data that will be read, the transformations that will be applied, and the sink feature group(s) for the data. Whether this a batch program or a streaming program. Will a transformation be used at runtime and require request-time parameters to be computed? If yes, then create as a custom transformation in Hopsworks and attach it to the feature group.

## Job
Do you need to first run a backfill job with start/end time for the data sources? Do the source feature groups or data sources support event_time?
Will it be a simple job execution or a scheduled job (optionally with incremental reads from feature groups and/or data sources). 

## Sink
One or more feature groups should be the sink of the feature pipelines.

# Data Processing Framework
Which framework was chosen based on expected workload size, feature freshness requirements, and user preferences.

# Testing Decisions

Can you save some sample input data that can be used to implement an integration test that reads the sample data, transforms it, writes it to a test feature group created when needed, read the data written, and then delete test feature group. Add unit test for transformations that should be contracts for downstream consumers of the engineered features.


---
name: hops-reqs
description: Create a specification for an ML system as a composition of datasources and separate feature, training, and inference (FTI) pipelines (batch or online). There will be an order relationship between the pipelines, some will be blocked-by other pipelines. Write down the specification as a markdown file in reqs/. Ask the user if they want to then implement the system.
---

# ML System Requirements

This skill should be invoked when the user wants to build an ML system and the output should be to create the specification for the ML system that can then be implemented.

The specification decomposes the ML system into separate **feature, training, and inference (FTI) pipelines** that are independently developed and operated, connected only through the feature store. This is the divide-and-conquer step of the MVPS (Minimum Viable Prediction Service) process: get to a working prediction service fast, then iterate. Do not specify one monolithic pipeline.

## Contract
- **Input:** a user description of the prediction problem, data sources, and ML-system type (batch or real-time).
- **Output:** an ordered ML-system specification written to `reqs/<ml-system-name>.md` (feature → training → inference pipelines with blocked-by relationships, each linking the implementing skill).
- **Pre-condition:** a Hopsworks project with discoverable data/feature groups (use hops CLI to suggest candidates).

## Ask the user

Use AskUserQuestion (step 1) for a description of the prediction problem, the data sources, and the ML-system type. Look at available features and datasources (use hops CLI) to suggest data that could be used. After writing the spec (step 5), ask the user whether to implement the system.

## Steps

1. Use AskUserQUestion for a description of the prediction problem, the KPI(s) it should improve and the ML proxy metric the model optimizes (this should correlate with the KPI), the data sources (all of them or specific sets of features), and the type of ML system (batch or real-time) to build. These four items plus how predictions are consumed (UI or API) and how the system is monitored form the AI-system card that summarizes the spec. Look at available features and datasources (use hops CLI) to suggest data that could be used. - Prediction problem types include 
  - binary classification
  - multiclass classification
  - regression
  - ranking
  - forecasting
  - anomaly detection

2. Identify any new candidate features not already available in feature groups. Prefer reusing existing features: the lowest-cost feature pipeline is the one you don't have to build. New features are computed by **model-independent transformations (MITs)** in feature pipelines and stored as reusable feature data in the feature store. Batch or streaming feature pipelines are ok; choose streaming when feature freshness requirements demand it. Then backfill feature data over historical data. Then schedule incremental batch feature pipelines to keep the feature data up to date. Load the **hops-features** skill to write out the feature pipeline specification (it hands off to hops-fg / hops-fv / hops-data-sources).

3. Analyze available data in an EDA phase. Select candidate features for the model and the model framework and define any **model-dependent transformations (MDTs)** needed (e.g., OHE, normalization, imputation). MDTs are specific to one model, so they are attached to the feature view rather than precomputed in feature pipelines, and the feature view applies them identically at training and inference time to avoid training/serving skew. Create a feature view for the selected features and attach the MDTs. Create training data with the feature view. If training data is expected to be greater than 10GB, AskUserQuestion on whether training should first be created as files with Spark (a training data pipeline). Then train the model in a Python program, unless it is a transformer model in which case you can AskUserQuestion about whether to use Ray or FSDP/Torch. If multiple ML frameworks could be used, AskUserQuestion on which one to use, providing a suggested on. The trained model should be evaluated - typical metrics for classification or regression should be computed and results should be saved as both JSON metrics and png files and saved to Hopsworks model registry along with a serialized copy of the model. Load the **hops-eda** skill for the analysis phase and the **hops-train** skill when implementing the training pipeline.

4. The inference pipeline defines the ML-system type (batch, online, or agentic). For batch ML systems, a batch inference pipeline will make predictions and save them to a feature group (load **hops-batch-inference** skill). Then a streamlit app (**hops-app**) can visualize those predictions. For real-time ML systems, the model will be deployed as a model deployment on Hopsworks (load **hops-online-inference** skill); features that depend on request-time parameters are computed in the online inference pipeline as **on-demand transformations (ODTs)**. In all cases the inference pipeline should log its inputs and predictions for monitoring and debugging. You may need to create a new Python environment and install dependencies for the model deployment. After the model has been deployed write a UI to use the model in a streamlit app deployed in Hopsworks (**hops-app**).

5. Write the specification to `reqs/<ml-system-name>.md`: the prediction problem and data sources, then the ordered pipelines (feature → training → inference) with their blocked-by relationships, each linking to the skill that implements it. Then ask the user whether to implement the system.
    

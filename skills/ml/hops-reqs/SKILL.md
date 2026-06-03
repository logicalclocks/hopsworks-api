---
name: hops-reqs
description: Create a specification an ML system as a composition of datasources, feature pipelines, training pipelines, and batch inference pipelines or online inference pipelines. There will be an order relationship between the pipelines, some will be blocked-by other pipelines. Write down the specification as a markdown file in reqs/. Ask the user if they want to then implement the system. Input: a user description of an ML system; Output: a reqs/<ml-system-name>.md spec.
---

# ML System Requirements

This skill should be invoked when the user wants to build an ML system and the output should be to create the specification for the ML system that can then be implemented.

## Contract
- **Input:** a user description of the prediction problem, data sources, and ML-system type (batch or real-time).
- **Output:** an ordered ML-system specification written to `reqs/<ml-system-name>.md` (feature → training → inference pipelines with blocked-by relationships, each linking the implementing skill).
- **Pre-condition:** a Hopsworks project with discoverable data/feature groups (use hops CLI to suggest candidates).

## Ask the user

Use AskUserQuestion (step 1) for a description of the prediction problem, the data sources, and the ML-system type. Look at available features and datasources (use hops CLI) to suggest data that could be used. After writing the spec (step 5), ask the user whether to implement the system.

## Steps

1. Use AskUserQUestion for a description of the prediction problem, the data sources (all of them or specific sets of features), and the type of ML system (batch or real-time) to build. Look at available features and datasources (use hops CLI) to suggest data that could be used. - Prediction problem types include 
  - binary classification
  - multiclass classification
  - regression
  - ranking
  - forecasting
  - anomaly detection

2. Identify any new candidate features not already available in feature groups that could be precomputed in feature pipelines. Batch or streaming feature pipelines are ok. Then backfill feature data. Then schedule incremental batch feature pipelines to keep the feature data up to date. Load the **hops-features** skill to write out the feature pipeline specification (it hands off to hops-fg / hops-fv / hops-data-sources).

3. Analyze available data in an EDA phase. Select candidate features for the model and the model framework and define any feature transformations needed (e.g., OHE, normalization, imputation, etc). Create a feature view for the selected features and apply the feature transformations. Create training data with the feature view. If training data is expected to be greater than 10GB, AskUserQuestion on whether training should first be created as files with Spark (a training data pipeline). Then train the model in a Python program, unless it is a transformer model in which case you can AskUserQuestion about whether to use Ray or FSDP/Torch. If multiple ML frameworks could be used, AskUserQuestion on which one to use, providing a suggested on. The trained model should be evaluated - typical metrics for classification or regression should be computed and results should be saved as both JSON metrics and png files and saved to Hopsworks model registry along with a serialized copy of the model. Load the **hops-eda** skill for the analysis phase and the **hops-train** skill when implementing the training pipeline.

4. For batch ML systems, a batch inference pipeline will make predictions and save them to a feature group (load **hops-batch-inference** skill). Then a streamlit app (**hops-app**) can visualize those predictions. For real-time ML systems, the model will be deployed as a model deployment on Hopsworks (load **hops-online-inference** skill). You may need to create a new Python environment and install dependencies for the model deployment. After the model has been deployed write a UI to use the model in a streamlit app deployed in Hopsworks (**hops-app**).

5. Write the specification to `reqs/<ml-system-name>.md`: the prediction problem and data sources, then the ordered pipelines (feature → training → inference) with their blocked-by relationships, each linking to the skill that implements it. Then ask the user whether to implement the system.
    

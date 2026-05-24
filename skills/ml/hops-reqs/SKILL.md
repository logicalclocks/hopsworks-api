---
name: create a ML system specification
description: Create a specification an ML system as a composition of datasources, feature pipelines, training pipelines, and batch inference pipelines or online inference pipelines. There will be an order relationship between the pipelines, some will be blocked-by other pipelines. Write down the specification as a markdown file in reqs/. Ask the user if they want to then implement the system. 
---

This skill should be invoked when the user wants to build an ML system and the output should be to create the specification for the ML system that can then be implemented.

1. Use AskUserQUestion for a description of the prediction problem, the data sources (all of them or specific sets of features), and the type of ML system (batch or real-time) to build. Look at available features and datasources (use hops CLI) to suggest data that could be used. - Prediction problem types include 
  - binary classification
  - multiclass classification
  - regression
  - ranking
  - forecasting
  - anomaly detection

2. Identify any new candidate features not already available in feature groups that could be precomputed in feature pipelines. Batch or streaming feature pipelines are ok. Then backfill feature data. Then schedule incremental batch feature pipelines to keep the feature data up to date. Load hops-feature-pipeline skill. Load hops-features skill to write out a feature pipeline specification.

3. Analyze available data in an EDA phase. Select candidate features for the model and the model framework and define any feature transformations needed (e.g., OHE, normalization, imputation, etc). Create a feature view for the selected features and apply the feature transformations. Create training data with the feature view. If training data is expected to be greater than 10GB, AskUserQuestion on whether training should first be created as files with Spark (a training data pipeline). Then train the model in a Python program, unless it is a transformer model in which case you can AskUserQuestion about whether to use Ray or FSDP/Torch. If multiple ML frameworks could be used, AskUserQuestion on which one to use, providing a suggested on. The trained model should be evaluated - typical metrics for classification or regression should be computed and results should be saved as both JSON metrics and png files and saved to Hopsworks model registry along with a serialized copy of the model. Load hops-training-pipeline skill. Load hops-training skill when implementing the training pipeline.

4. For batch ML systems, a batch inference pipeline will make predictions and save them to a feature group (load hops-batch-inference skill). Then a streamlit app (hops-apps) can visualize those predictions. For real-time ML systems, the model will be deployed as a model deployment on Hopsworks (load hops-online-inference skill). You may need to create a new Python environment and install dependencies for the model deployment. After the model has been deployed write a UI to use the model in a streamlit app deployed in Hopsworks. 
    

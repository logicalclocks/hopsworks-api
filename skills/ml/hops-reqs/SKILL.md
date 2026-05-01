---
name: build a ML system
description: Create an architecture for an ML system as a composition of feature groups, feature pipelines, training pipelines, and batch inference pipelines or online inference pipelines. There will be an order relationship between the pipelines, some will be blocked-by other pipelines. Write it down as a markdown file in reqs/. Use when the user wants to create an ML system.
---

This skill should be invoked when the user wants to build an ML system.

1. Ask the user for a description of the prediction problem and the data sources. Look at available feature groups and for any data sources (connnectors) see if there might be tables that could be mounted as external feature groups.

2. Identify any new candidate features not already available in feature groups that could be precomputed in feature pipelines. Batch or streaming feature pipelines are ok. Then backfill feature data. Then schedule incremental batch feature pipelines to keep the feature data up to date.

3. Analyze available data in an EDA phase. Read the summary of the EDA. Select candidate features for the model and the model framework. Create a feature view for the selected features and apply transformations based on numerical feature's data distributions and whether a feature is a categorical feature and impute missing data. The create training data with the feature view and train the model. If training data is too large to fit in a Pandas Dataframe, run the training pipelines as a spark job that saves training data as files. Otherwise, it should be a big Python program. The training program is typically written in Python using a ML framework. The trained model should be evaluated - typical metrics for classification or regression should be computed along with png files and saved to Hopsworks model registry along with a serialized copy of the model.

4. For batch ML systems, a batch inference pipeline will make predictions and save them to a feature group. Then an app or dashboard can visualize those predictions. For real-time ML systems, the model will be deployed as a deployment on Hopsworks. You may need to create a new Python environment and install dependencies for that. After the model has been deployed write a UI to show it off as a streamlit app in Hopsworks and deploy it in Hopsworks.
    
5. For each of the pipelines/programs you have to write, create an issue in Github. Track dependencies (blockers) and make a kanban board with all of them.

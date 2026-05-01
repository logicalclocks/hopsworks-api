---
name: hopsworks-train
description: Use when training an ML model. Identify candidate features, then load, explore, and visualize their data from the feature store.
  Select features for inclusion in a feature view. Use the feature view to create training data. Train a model with an appropriate ML framework.
  Evaluate the model and register the model and its evaluation (pngs, metrics) in the Hopsworks model registry.
model: claude-sonnet-4-6
allowed-tools: Read, Grep, Glob, Edit, Write, Bash
---


---

## 1. EDA (Exploratory Data Analysis)

**Notebook**: `eda.ipynb` (executed output: `eda_output.ipynb`)

**Run with papermill**:
```bash
uv pip install --python /srv/hops/venv/bin/python3 papermill ipykernel
python3 -m ipykernel install --user --name python3
python3 -m papermill eda.ipynb eda_output.ipynb --no-progress-bar
```

**Key steps**:
- Connect to Hopsworks and load all 7 feature groups as Pandas DataFrames
- Inspect shapes, dtypes, and head of each feature group
- Analyze label distribution (`final_fraud_flag` in `fraud_derived_features`,
  `drvd_trans_frd_ind` in `trans_card_authrzn_sig_tran_aggr`)
- Identify missing data per column per feature group
- Plot distributions of continuous, discrete, and categorical features
- Compute correlations with fraud label
- Compare feature distributions for fraud vs legitimate transactions

**Key EDA findings**:
- `final_fraud_flag` is 100% NULL in `fraud_derived_features` (cross-table join
  produced NULLs because source FGs were generated independently)
- 5 cross-table columns are 100% NULL: `curr_spend_ratio`, `queued_to_total_ratio`,
  `dispute_per_device`, `devices_per_card_ratio`, `final_fraud_flag`
- `wkly_sig_trans_amt` / `wkly_sig_trans_spnd_ratio` are ~81% NULL
- `amt_zscore_vs_card` has 0.6% NULL (imputed with median)
- `drvd_trans_frd_ind` in `trans_card_authrzn_sig_tran_aggr` has ~0.8% fraud rate
- 30+ non-null engineered features available in `fraud_derived_features`

---

## 2. Feature View Creation

**Script**: `train_isolation_forest.py` (Step 2)

**Hopsworks API pattern**:
```python
fg_derived = fs.get_feature_group("fraud_derived_features", version=1)
fg_trans = fs.get_feature_group("trans_crd_authrztn", version=1)

query = fg_derived.select(DERIVED_FEATURES).join(
    fg_trans.select(TRANS_FEATURES),
    on=["trans_id"],  # common join key (PK of both FGs)
)

fv = fs.get_or_create_feature_view(
    name="fraud_isolation_forest_fv",
    version=1,
    query=query,
    transformation_functions=[...],
    description="...",
)
```

**Join strategy**: Both `fraud_derived_features` and `trans_crd_authrztn` share
`trans_id` as their primary key, making it the natural join key.

**Features selected** (37 total):
- 32 from `fraud_derived_features`: engineered amount/risk/velocity/time/channel
  features (excluding 100% and 81% NULL columns)
- 5 from `trans_crd_authrztn`: raw risk scores (`vaa_adv_auth_risk_scr`,
  `trans_curr_frd_falcon_scr`), categorical (`trans_auth_ntwrk_id`,
  `mrch_ind_clsfcn_code`), velocity (`v24_hr_trans_cnt`)

---

## 3. Feature Transformations

**Built-in transformations from `hsfs.builtin_transformations`**:

| Transformation | Features | Why |
|---|---|---|
| `impute_median` | `amt_zscore_vs_card` | 0.6% missing values |
| `robust_scaler` | 14 continuous features | Handles outliers in amount, distance, time features; IQR-based scaling |
| `label_encoder` | `trans_auth_ntwrk_id`, `mrch_ind_clsfcn_code` | Convert categorical strings to integers for model compatibility |

**Hints**:
- Import: `from hsfs.builtin_transformations import robust_scaler, label_encoder, impute_median`
- Each function takes a feature name: `robust_scaler("trans_amt_log")`
- Transformations are applied during `train_test_split()` and at serving time
- Statistics (median, IQR, category mappings) are computed from training data
- After transformations, fill any remaining NaNs with `fillna(0)` as a safety net

---

## 4. Train/Test Split

**Hopsworks API pattern**:
```python
X_train, X_test, _, _ = fv.train_test_split(
    test_size=0.2,
    description="80/20 random split for isolation forest",
)
```

**Hints**:
- Returns `(X_train, X_test, y_train, y_test)` tuple (y is empty if no labels)
- Transformations are automatically applied to the returned DataFrames
- Statistics are computed and stored with training dataset version
- Call `fv.get_train_test_split(training_dataset_version=1)` to retrieve later

---

## 5. Train Isolation Forest

**scikit-learn pattern**:
```python
from sklearn.ensemble import IsolationForest

model = IsolationForest(
    n_estimators=200,
    contamination=0.008,  # matches observed ~0.8% fraud rate
    random_state=42,
    n_jobs=-1,
)
model.fit(X_train)
```

**Hints**:
- `contamination` should approximate the expected anomaly rate from EDA
- `decision_function()` returns anomaly scores (lower = more anomalous)
- `predict()` returns -1 for anomalies, 1 for normal
- Convert to binary: `y_pred = (model.predict(X_test) == -1).astype(int)`
- `model.offset_` is the decision threshold

---

## 6. Evaluation

**Evaluation labels**: Since `final_fraud_flag` is all NULL in the feature view,
load `drvd_trans_frd_ind` from `trans_card_authrzn_sig_tran_aggr` separately.
This table was generated independently from the same population, simulating
delayed fraud labels from a separate investigation pipeline.

**Metrics computed**:
- Anomaly rate, mean/std anomaly scores
- Precision, recall, F1 against ground truth
- ROC AUC, Average Precision (using `-anomaly_scores` for ranking)
- Feature importance (mean absolute difference between anomaly and normal groups)

**Evaluation plots saved as PNGs**:
- `anomaly_score_distribution.png` - Score histograms for normal vs anomaly
- `confusion_matrix.png` - TP/FP/TN/FN counts
- `roc_curve.png` - ROC with AUC
- `precision_recall_curve.png` - PR with AP
- `feature_importance.png` - Top 20 discriminative features

---

## 7. Model Registration

**Save model locally with joblib**:
```python
import joblib
joblib.dump(model, "fraud_isolation_forest_model/isolation_forest_model.pkl")
```

**Register in Hopsworks Model Registry**:
```python
mr = project.get_model_registry()
hw_model = mr.python.create_model(
    name="fraud_isolation_forest",
    metrics=metrics,          # dict of evaluation metrics
    description="...",
    input_example=X_train.head(1),
    feature_view=fv,          # provenance link
    training_dataset_version=1,
)
hw_model.save("fraud_isolation_forest_model")  # uploads dir including images/
```

**Hints**:
- `model.save()` moves files to registry by default; use `keep_original_files=True`
  to keep local copies
- `metrics` dict is stored with the model and queryable via
  `mr.get_best_model("fraud_isolation_forest", metric="roc_auc", direction="max")`
- Download model files later: `model.download(local_path="./model")`

---

## 8. Project File Layout

```
pnc/
  generate_feature_data.py         # Synthetic data generation (5 source FGs)
  compute_derived_features.py      # Feature engineering (2 derived FGs)
  eda.ipynb                        # EDA notebook (source)
  eda_output.ipynb                 # EDA notebook (executed)
  train_isolation_forest.py        # Training pipeline (FV + model + registry)
  requirements.txt                 # Data generation dependencies
  training-requirements.txt        # Training pipeline dependencies
  SKILLS.md                        # This file
  fraud_isolation_forest_model/    # Model artifacts (moved to registry)
    isolation_forest_model.pkl
    feature_list.json
    images/
      anomaly_score_distribution.png
      confusion_matrix.png
      roc_curve.png
      precision_recall_curve.png
      feature_importance.png
```

---

## 9. Feature Groups Reference

| Feature Group | PK | Rows | Key Features |
|---|---|---|---|
| `trans_crd_authrztn` | trans_id | 1M | Raw authorization: amounts, risk scores, MCC, network, CVV |
| `trans_card_authrzn_sig_tran_aggr` | crd_num, trans_dt | 1M | Aggregations: queued counts, disputes, fraud indicators |
| `dbt_crd_trans_dly_aggr_crd` | crd_num, aggr_dt | ~97K | Daily: weekly spend amounts and ratios |
| `acct_ttl_tax_cert` | acct_id | 50K | Account reference: close dates |
| `debit` | event_id | ~500K | Non-monetary events: phone/address changes, lost cards |
| `fraud_derived_features` | trans_id | 1M | 43 engineered features for isolation forest |
| `account_risk_profile` | acct_id | ~20K | Per-account risk: event velocity, change flags |

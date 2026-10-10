# ruff: noqa: INP001
"""The fraud example's training pipeline: an XGBoost classifier behind an sklearn preprocessor.

Run as the job `<slug>-train` in `<slug>-jobs-env`, after the features and the
aggregates backfill:

    hops job deploy <slug>-train hdfs:///Projects/<project>/<system dir>/ccfraud/train_fraud.py \
        --env <slug>-jobs-env --args "--test-days 7" --run --wait

It creates the feature view `cc_fraud_fv` over `cc_trans_fg` (the label and the
per-transaction features), `merchant_details`, and the card's sliding-window
aggregates in `cc_trans_aggs_fg` v2 joined with `account_details` and
`bank_details`, with feature logging on. The transactions of the last
`--test-days` days are the test set. One sklearn Pipeline (median imputation,
ordinal encoding of the categoricals, XGBoost weighted by the class imbalance)
is trained on the raw features and registered as `cc_fraud_xgboost_model`,
with PR-AUC, precision, recall, F1 and accuracy, and the deployment's
`predictor.py` in its files.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import hopsworks
import joblib
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    auc,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder


MODEL = "cc_fraud_xgboost_model"
FEATURE_VIEW = "cc_fraud_fv"
CATEGORICAL = ["category", "country", "bank_country"]
PREDICTOR = Path(__file__).resolve().parent / "predictor.py"


def _feature_view(fs):
    merchants = fs.get_feature_group("merchant_details", version=1)
    accounts = fs.get_feature_group("account_details", version=1)
    banks = fs.get_feature_group("bank_details", version=1)
    aggs = fs.get_feature_group("cc_trans_aggs_fg", version=2)
    transactions = fs.get_feature_group("cc_trans_fg", version=1)
    card = (
        aggs.select_except(["cc_num", "account_id", "bank_id", "event_time"])
        .join(accounts.select(["debt_end_prev_month"]), on="account_id")
        .join(
            banks.select(["credit_rating", "days_since_bank_cr_changed", "country"]),
            prefix="bank_",
            on="bank_id",
        )
    )
    query = (
        transactions.select_except(
            ["t_id", "cc_num", "merchant_id", "account_id", "ip_address", "ts"]
        )
        .join(merchants.select_features(), on="merchant_id", join_type="inner")
        .join(card, on="cc_num")
    )
    return fs.get_or_create_feature_view(
        name=FEATURE_VIEW,
        version=1,
        description="Features of a credit card transaction for predicting whether it is fraud",
        query=query,
        labels=["is_fraud"],
        inference_helper_columns=["prev_card_present", "prev_ip_address", "prev_ts"],
        logging_enabled=True,
    )


def _pipeline(numeric: list[str], scale_pos_weight: float) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numeric),
            (
                "cat",
                Pipeline(
                    [
                        (
                            "imputer",
                            SimpleImputer(strategy="constant", fill_value="UNKNOWN"),
                        ),
                        (
                            "encoder",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value", unknown_value=-1
                            ),
                        ),
                    ]
                ),
                CATEGORICAL,
            ),
        ],
        verbose_feature_names_out=False,
    )
    model = xgb.XGBClassifier(
        scale_pos_weight=scale_pos_weight,
        max_depth=6,
        learning_rate=0.1,
        n_estimators=100,
        eval_metric="aucpr",
        random_state=42,
    )
    return Pipeline([("preprocessor", preprocessor), ("model", model)])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--test-days",
        type=int,
        default=7,
        help="Days of the most recent transactions held out as the test set",
    )
    args = parser.parse_args()

    project = hopsworks.login()
    fs = project.get_feature_store()
    fv = _feature_view(fs)
    test_start = (datetime.now(timezone.utc) - timedelta(days=args.test_days)).strftime(
        "%Y-%m-%d %H:%M"
    )
    X_train, X_test, y_train, y_test = fv.train_test_split(test_start=test_start)
    y_train = y_train["is_fraud"].astype(bool)
    y_test = y_test["is_fraud"].astype(bool)
    positives = int(y_train.sum())
    if positives == 0 or not y_test.any():
        raise SystemExit(
            f"No fraud in the training or the test set (train {positives}, test {int(y_test.sum())}):"
            " backfill more history or change --test-days"
        )
    scale_pos_weight = (len(y_train) - positives) / positives
    numeric = [c for c in X_train.columns if c not in CATEGORICAL]
    pipeline = _pipeline(numeric, scale_pos_weight)
    pipeline.fit(X_train, y_train)

    predicted = pipeline.predict(X_test)
    curve_precision, curve_recall, _ = precision_recall_curve(
        y_test, pipeline.predict_proba(X_test)[:, 1]
    )
    metrics = {
        "pr_auc": round(float(auc(curve_recall, curve_precision)), 4),
        "precision": round(float(precision_score(y_test, predicted)), 4),
        "recall": round(float(recall_score(y_test, predicted)), 4),
        "f1_score": round(float(f1_score(y_test, predicted)), 4),
        "accuracy": round(float(accuracy_score(y_test, predicted)), 4),
    }
    print(
        f"{len(X_train):,} training rows ({positives} fraud), test from {test_start}: {metrics}"
    )

    model_dir = Path(tempfile.mkdtemp()) / MODEL
    model_dir.mkdir()
    joblib.dump(pipeline, model_dir / "cc_fraud_pipeline.pkl")
    shutil.copy(PREDICTOR, model_dir / PREDICTOR.name)
    model = project.get_model_registry().python.create_model(
        name=MODEL,
        metrics=metrics,
        feature_view=fv,
        description=(
            "Credit card fraud: an sklearn Pipeline of imputation, ordinal encoding and XGBoost, "
            f"trained on {len(X_train):,} transactions with {positives} fraud"
        ),
    )
    model.save(str(model_dir))
    print(f"Registered {MODEL} v{model.version}")


if __name__ == "__main__":
    main()

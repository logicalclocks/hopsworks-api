# ruff: noqa: INP001
"""Train the XGBoost leasing classifier on the feature view and register it with its charts.

    python train.py [--test-size 0.2] [--context-rows 512] [--system leasing-example]

Creates the feature view leasing_decisions on its first run: every order with its
machine, the customer's credit report and the customer's last settled lease, both as
they were when the order was placed (point-in-time joins on the event times), and the
custom model-dependent transformations of transformations.py, which compute every
feature the models read. Then it splits the orders, trains the classifier on whether
an order was leased, and registers leasing_xgboost with its metrics and evaluation
charts. The app runs this job again to retrain on the orders its simulation adds;
each run is a new training dataset and model version.

The model's files also carry `kumo_context.parquet`, a class-balanced sample of the
transformed training rows with their label: the context the app gives Kumo Tabular,
the pretrained in-context classifier, so both models decide from the same features.
"""

import argparse
import json
import tempfile
from pathlib import Path

import hopsworks
import matplotlib
import names
import numpy as np
import pandas as pd
import transformations
import xgboost as xgb
from sklearn import metrics


matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402 - after the backend is chosen


# Logged with every prediction the app makes, beside the features and XGBoost's decision.
EXTRA_LOG_COLUMNS = [
    {"name": "xgboost_probability", "type": "double"},
    {"name": "kumo_probability", "type": "double"},
    {"name": "kumo_leased", "type": "int"},
]


def feature_view(fs, system: str):
    """leasing_decisions: the orders joined point-in-time with the machine, the report and the last lease."""
    groups = {
        name: fs.get_feature_group(name, version=1)
        for name in (names.ORDERS, names.EQUIPMENT, names.CREDIT, names.OUTCOMES)
    }
    missing = [name for name, fg in groups.items() if fg is None]
    if missing:
        raise RuntimeError(f"Run generate_data.py first: {', '.join(missing)} missing")
    query = (
        groups[names.ORDERS]
        .select(["customer_id", "equipment_id", "lease_months", names.LABEL])
        .join(
            groups[names.EQUIPMENT].select(
                ["monthly_price", "replacement_value", "category", "power_source"]
            ),
            on=["equipment_id"],
        )
        .join(
            groups[names.CREDIT].select(
                [
                    "customer_type",
                    "credit_score",
                    "monthly_income",
                    "total_debt",
                    "delinquencies_12m",
                    "open_credit_lines",
                    "bankruptcies",
                ]
            ),
            on=["customer_id"],
        )
        .join(
            groups[names.OUTCOMES].select(
                ["outcome", "paid", "problem_free", "days_late", "damage_cost"]
            ),
            on=["customer_id"],
            prefix="prev_",
        )
    )
    return fs.get_or_create_feature_view(
        name=names.FEATURE_VIEW,
        version=1,
        query=query,
        labels=[names.LABEL],
        # Who and what an order is for: logged with each prediction, never a model input.
        inference_helper_columns=["customer_id", "equipment_id"],
        transformation_functions=transformations.attached(),
        logging_enabled=True,
        extra_log_columns=EXTRA_LOG_COLUMNS,
        logging_transport="realtime",
        description=f"{system}: lease orders with the machine, the customer's credit report and last "
        "lease as they were at the order, and the model-dependent transformations of the leasing models.",
    )


def kumo_context(X: pd.DataFrame, y: pd.Series, rows: int, seed: int) -> pd.DataFrame:
    """Up to `rows` training rows with their label, half leased and half not where the data allows."""
    frame = X.assign(**{names.LABEL: y.to_numpy()})
    half = rows // 2
    parts = [
        group.sample(min(len(group), half), random_state=seed)
        for _, group in frame.groupby(names.LABEL)
    ]
    return pd.concat(parts).sample(frac=1, random_state=seed).reset_index(drop=True)


def charts(
    directory: Path, model, X_test: pd.DataFrame, y_test: pd.Series, scores: np.ndarray
) -> None:
    """ROC, precision-recall, confusion matrix, calibration, score distribution and feature importance."""
    images = directory / "images"
    images.mkdir(parents=True, exist_ok=True)
    displays = {
        "roc_curve": metrics.RocCurveDisplay.from_predictions(y_test, scores),
        "precision_recall_curve": metrics.PrecisionRecallDisplay.from_predictions(
            y_test, scores
        ),
        "confusion_matrix": metrics.ConfusionMatrixDisplay.from_predictions(
            y_test, (scores >= 0.5).astype(int), display_labels=["not leased", "leased"]
        ),
    }
    for name, display in displays.items():
        display.figure_.savefig(images / f"{name}.png", dpi=120, bbox_inches="tight")
        plt.close(display.figure_)
    from sklearn.calibration import CalibrationDisplay

    calibration = CalibrationDisplay.from_predictions(y_test, scores, n_bins=10)
    calibration.figure_.savefig(
        images / "calibration.png", dpi=120, bbox_inches="tight"
    )
    plt.close(calibration.figure_)

    figure, axis = plt.subplots(figsize=(6, 4))
    for label, name in ((0, "not leased"), (1, "leased")):
        axis.hist(scores[y_test.to_numpy() == label], bins=30, alpha=0.6, label=name)
    axis.set_xlabel("XGBoost probability of leasing")
    axis.legend()
    figure.savefig(images / "score_distribution.png", dpi=120, bbox_inches="tight")
    plt.close(figure)

    importance = pd.Series(
        model.feature_importances_, index=X_test.columns
    ).sort_values()
    figure, axis = plt.subplots(figsize=(6, 0.3 * len(importance) + 1))
    importance.plot.barh(ax=axis)
    axis.set_xlabel("importance (gain)")
    figure.savefig(images / "feature_importance.png", dpi=120, bbox_inches="tight")
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument(
        "--context-rows", type=int, default=512, help="rows of Kumo Tabular's context"
    )
    parser.add_argument("--seed", type=int, default=7)
    # Every asset's description starts with the system's slug, which is how deleting the
    # system with its assets tells its own from another's.
    parser.add_argument("--system", default="leasing-example", help="the system's slug")
    args = parser.parse_args()

    project = hopsworks.login()
    fv = feature_view(project.get_feature_store(), args.system)
    # Transformed by the feature view: every column is a model input.
    X_train, X_test, y_train, y_test = fv.train_test_split(test_size=args.test_size)
    y_train, y_test = y_train[names.LABEL].astype(int), y_test[names.LABEL].astype(int)
    print(
        f"{len(X_train):,} training and {len(X_test):,} test orders, {y_train.mean():.0%} leased"
    )

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.06,
        subsample=0.9,
        colsample_bytree=0.9,
        eval_metric="logloss",
        random_state=args.seed,
    )
    model.fit(X_train, y_train)
    scores = model.predict_proba(X_test)[:, 1]
    decided = (scores >= 0.5).astype(int)
    results = {
        "roc_auc": float(metrics.roc_auc_score(y_test, scores)),
        "pr_auc": float(metrics.average_precision_score(y_test, scores)),
        "accuracy": float(metrics.accuracy_score(y_test, decided)),
        "f1": float(metrics.f1_score(y_test, decided)),
        "training_orders": float(len(X_train)),
    }
    print(json.dumps(results, indent=2))

    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        model.save_model(directory / "model.json")
        (directory / "features.json").write_text(json.dumps(list(X_train.columns)))
        kumo_context(X_train, y_train, args.context_rows, args.seed).to_parquet(
            directory / "kumo_context.parquet"
        )
        try:
            charts(directory, model, X_test, y_test, scores)
        except Exception as exc:  # noqa: BLE001 - a chart never stops the registration
            print(f"Charts failed: {exc}")
        registered = project.get_model_registry().python.create_model(
            name=names.MODEL,
            metrics=results,
            description=f"{args.system}: XGBoost classifier deciding whether a gardening machine is leased "
            "to a customer, on the leasing_decisions feature view's transformed features.",
            input_example=X_train.head(1),
            feature_view=fv,
        )
        registered.save(str(directory))
    print(
        f"Registered {names.MODEL} v{registered.version}: ROC AUC {results['roc_auc']:.3f}"
    )


if __name__ == "__main__":
    main()

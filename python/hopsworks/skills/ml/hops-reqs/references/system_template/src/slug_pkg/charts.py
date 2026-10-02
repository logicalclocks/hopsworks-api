"""Model performance charts, saved as PNG files under `images/` in the model directory.

The training pipeline calls `save_charts` before it registers a model, so the charts
are uploaded with the model and shown with it in the model registry. They are drawn
from the part the model was scored on (validation for research runs, test otherwise).

Classification: ROC curve, precision-recall curve, confusion matrix at the 0.5
threshold, calibration, score distribution per class. Regression: predicted against
actual, residuals. Both: feature importance, when the model exposes it.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np

_logger = logging.getLogger(__name__)


def _curves(y: np.ndarray, s: np.ndarray) -> tuple[np.ndarray, ...]:
    """False and true positive rates, precision and recall, at every distinct score."""
    order = np.argsort(-s, kind="mergesort")
    y, s = y[order], s[order]
    last_of_each_score = np.r_[np.flatnonzero(np.diff(s)), len(s) - 1]
    tp = np.cumsum(y)[last_of_each_score]
    fp = (last_of_each_score + 1) - tp
    positives, negatives = max(y.sum(), 1), max(len(y) - y.sum(), 1)
    fpr = np.r_[0.0, fp / negatives]
    tpr = np.r_[0.0, tp / positives]
    precision = np.r_[1.0, tp / (tp + fp)]
    recall = np.r_[0.0, tp / positives]
    return fpr, tpr, precision, recall


def _importance(model: Any, features: list[str]) -> tuple[list[str], np.ndarray] | None:
    values = getattr(model, "feature_importances_", None)
    if values is None and getattr(model, "coef_", None) is not None:
        values = np.abs(np.asarray(model.coef_)).ravel()
    if values is None or len(values) != len(features):
        return None
    top = np.argsort(values)[-20:]
    return [features[i] for i in top], np.asarray(values)[top]


def _classification(plt: Any, y: np.ndarray, s: np.ndarray, out: Path) -> None:
    fpr, tpr, precision, recall = _curves(y, s)

    fig, ax = plt.subplots(figsize=(5, 5))
    auc = float(np.sum(np.diff(fpr) * (tpr[1:] + tpr[:-1]) / 2))
    ax.plot(fpr, tpr, label=f"AUC {auc:.3f}")
    ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
    ax.set(xlabel="False positive rate", ylabel="True positive rate", title="ROC curve")
    ax.legend(loc="lower right")
    fig.savefig(out / "roc_curve.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot(recall, precision)
    ax.axhline(y.mean(), linestyle="--", color="grey", label=f"base rate {y.mean():.3f}")
    ax.set(xlabel="Recall", ylabel="Precision", title="Precision-recall curve")
    ax.legend(loc="upper right")
    fig.savefig(out / "precision_recall_curve.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    predicted = s >= 0.5
    matrix = np.array(
        [
            [np.sum(~predicted & (y == 0)), np.sum(predicted & (y == 0))],
            [np.sum(~predicted & (y == 1)), np.sum(predicted & (y == 1))],
        ]
    )
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.imshow(matrix, cmap="Blues")
    for (i, j), count in np.ndenumerate(matrix):
        dark = count > matrix.max() / 2
        ax.text(
            j,
            i,
            f"{count:,}",
            ha="center",
            va="center",
            color="white" if dark else "black",
        )
    ax.set(
        xticks=[0, 1],
        yticks=[0, 1],
        xticklabels=["0", "1"],
        yticklabels=["0", "1"],
        xlabel="Predicted",
        ylabel="Actual",
        title="Confusion matrix (threshold 0.5)",
    )
    fig.savefig(out / "confusion_matrix.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    bins = np.clip((s * 10).astype(int), 0, 9)
    filled = [b for b in range(10) if np.any(bins == b)]
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot(
        [s[bins == b].mean() for b in filled],
        [y[bins == b].mean() for b in filled],
        marker="o",
    )
    ax.plot([0, 1], [0, 1], linestyle="--", color="grey")
    ax.set(xlabel="Mean predicted probability", ylabel="Observed rate", title="Calibration")
    fig.savefig(out / "calibration.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(s[y == 0], bins=40, alpha=0.6, label="0", density=True)
    ax.hist(s[y == 1], bins=40, alpha=0.6, label="1", density=True)
    ax.set(xlabel="Predicted probability", ylabel="Density", title="Scores by actual class")
    ax.legend()
    fig.savefig(out / "score_distribution.png", dpi=120, bbox_inches="tight")
    plt.close(fig)


def _regression(plt: Any, y: np.ndarray, p: np.ndarray, out: Path) -> None:
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.scatter(y, p, s=4, alpha=0.4)
    low, high = float(min(y.min(), p.min())), float(max(y.max(), p.max()))
    ax.plot([low, high], [low, high], linestyle="--", color="grey")
    ax.set(xlabel="Actual", ylabel="Predicted", title="Predicted against actual")
    fig.savefig(out / "predicted_vs_actual.png", dpi=120, bbox_inches="tight")
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(p - y, bins=50)
    ax.axvline(0, linestyle="--", color="grey")
    ax.set(xlabel="Predicted minus actual", ylabel="Rows", title="Residuals")
    fig.savefig(out / "residuals.png", dpi=120, bbox_inches="tight")
    plt.close(fig)


def save_charts(
    model_dir: Path,
    task: str,
    model: Any,
    features: list[str],
    y_true: Any,
    y_pred: Any,
) -> list[str]:
    """Write the task's charts to `model_dir/images/` and return their file names.

    `y_pred` is what evaluate.py scores: the positive-class probability for
    classification, the prediction for regression. A chart that cannot be drawn is
    logged and skipped, so a missing plotting library never stops a registration.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        _logger.warning("matplotlib is not installed; the model is registered without charts")
        return []
    out = Path(model_dir) / "images"
    out.mkdir(parents=True, exist_ok=True)
    y = np.asarray(y_true, dtype=float).ravel()
    p = np.asarray(y_pred, dtype=float).ravel()
    try:
        if task == "classification":
            _classification(plt, y, p, out)
        else:
            _regression(plt, y, p, out)
        importance = _importance(model, features)
        if importance is not None:
            names, values = importance
            fig, ax = plt.subplots(figsize=(6, max(3, 0.3 * len(names))))
            ax.barh(names, values)
            ax.set(xlabel="Importance", title="Feature importance")
            fig.savefig(out / "feature_importance.png", dpi=120, bbox_inches="tight")
            plt.close(fig)
    except Exception as e:  # noqa: BLE001 - charts are a courtesy, never a failure
        _logger.warning("could not draw the model charts: %s", e)
    return sorted(f.name for f in out.glob("*.png"))

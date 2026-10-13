"""The fraud model's features, training and scoring, shared by train.py, predictor.py and the app.

An Isolation Forest scores a card's live window (from Spark Structured
Streaming) against its spending baseline (from dbt): the window's amounts,
velocity and distance divided by the baseline's say how unusual the hour is
for the card. A tumbling window fills up over its hour, so its counts and sums
are taken per hour of the window elapsed so far: a card scored late in the hour
is comparable with the cards the model trained on early in it. The model is
unsupervised, so it needs no labels at serving time; risk is the anomaly score
rescaled to 0-1 on the training data's 5th and 99.9th percentiles.
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

MODEL_NAME = "card_fraud_model"
MODEL_FILE = "card_fraud_model.pkl"
FEATURE_VIEW = "card_fraud_fv"

WINDOW_FEATURES = (
    "txn_count",
    "approved_count",
    "declined_count",
    "decline_ratio",
    "queued_count",
    "amount_sum_eur",
    "amount_avg_eur",
    "amount_stddev_eur",
    "amount_max_eur",
    "amount_min_eur",
    "declined_amount_eur",
    "cash_withdrawals_sum_eur",
    "distinct_merchants",
    "distinct_regions",
    "distinct_entry_modes",
    "max_merchant_distance_km",
    "high_risk_count",
    "avg_issuer_fraud_score",
    "max_issuer_fraud_score",
    "avg_network_risk_score",
    "max_network_risk_score",
)
# When the window closes and how long it is, which turn its counts into rates.
WINDOW_TIME = ("window_end", "window_seconds")
# Counted or summed over the window, so taken per hour of it elapsed.
PER_HOUR = (
    "txn_count",
    "approved_count",
    "declined_count",
    "queued_count",
    "amount_sum_eur",
    "declined_amount_eur",
    "cash_withdrawals_sum_eur",
    "high_risk_count",
)
# A window opened moments ago would turn one authorisation into a huge rate.
MIN_ELAPSED_SECONDS = 600
BASELINE_FEATURES = (
    "avg_daily_amount_eur_30d",
    "avg_daily_txn_count_7d",
    "decline_ratio_30d",
    "high_risk_ratio_30d",
    "max_merchant_distance_km_30d",
)
# The window over the baseline; a card without a baseline gets the training median.
RATIOS = {
    "amount_to_baseline": ("amount_sum_eur", "avg_daily_amount_eur_30d"),
    "txn_count_to_baseline": ("txn_count", "avg_daily_txn_count_7d"),
    "distance_to_baseline": ("max_merchant_distance_km", "max_merchant_distance_km_30d"),
}
FEATURE_COLUMNS = WINDOW_FEATURES + BASELINE_FEATURES + tuple(RATIOS)


def elapsed_hours(frame: pd.DataFrame, now: datetime | None = None) -> np.ndarray:
    """The hours of each row's window passed at `now` (UTC), at least MIN_ELAPSED_SECONDS.

    A window that has closed counts in full; a row without window times counts as a full hour.
    """
    now = pd.Timestamp(now or datetime.now(UTC)).tz_convert(None)
    seconds = pd.to_numeric(frame["window_seconds"], errors="coerce").fillna(3600)
    end = frame["window_end"]
    # The online store returns timestamps as datetimes, a feature vector may as epoch milliseconds.
    unit = "ms" if pd.api.types.is_numeric_dtype(end) else None
    end = pd.to_datetime(end, errors="coerce", utc=True, unit=unit).dt.tz_convert(None)
    passed = (now - (end - pd.to_timedelta(seconds, unit="s"))).dt.total_seconds()
    passed = passed.fillna(seconds).clip(lower=MIN_ELAPSED_SECONDS)
    return np.minimum(passed, seconds).to_numpy() / 3600


def prepare_features(
    frame: pd.DataFrame, medians: dict[str, float] | None = None, now: datetime | None = None
):
    """The model's input from window and baseline columns, and the medians that filled its gaps.

    `frame` carries window_end and window_seconds beside the features; without
    them the counts are taken as a full hour's. Pass the medians training
    returned to fill a served row the same way.
    """
    features = frame.reindex(columns=WINDOW_FEATURES + BASELINE_FEATURES).apply(
        pd.to_numeric, errors="coerce"
    )
    if set(WINDOW_TIME) <= set(frame.columns):
        hours = elapsed_hours(frame, now)
        for name in PER_HOUR:
            features[name] = features[name] / hours
    for name, (numerator, denominator) in RATIOS.items():
        features[name] = features[numerator] / features[denominator].replace(0, np.nan)
    features = features.replace([np.inf, -np.inf], np.nan)
    if medians is None:
        medians = {k: float(v) for k, v in features.median().fillna(0.0).items()}
    return features.fillna(pd.Series(medians)).fillna(0.0).astype(float), medians


def train(frame: pd.DataFrame, contamination: float = 0.02, random_state: int = 42) -> dict:
    """The model artifact, trained on cards' window and baseline rows."""
    if not 0 < contamination <= 0.5:
        raise ValueError("contamination must be in (0, 0.5]")
    if len(frame) < 50:
        raise ValueError("at least 50 cards are needed to train")
    features, medians = prepare_features(frame)
    model = IsolationForest(
        n_estimators=200, contamination=contamination, random_state=random_state, n_jobs=-1
    ).fit(features)
    anomaly = -model.decision_function(features)
    low, high = float(np.quantile(anomaly, 0.05)), float(np.quantile(anomaly, 0.999))
    return {
        "model": model,
        "feature_names": list(FEATURE_COLUMNS),
        "medians": medians,
        "calibration_low": low,
        "calibration_high": high if high > low else low + 1.0,
        "contamination": contamination,
        "training_rows": len(features),
        "trained_at": datetime.now(UTC).isoformat(),
    }


def score(artifact: dict, frame: pd.DataFrame, now: datetime | None = None):
    """(risk in 0-1, the model's anomaly flag, raw anomaly score) per row of `frame`."""
    if artifact.get("feature_names") != list(FEATURE_COLUMNS):
        raise ValueError("the model was trained on other features than this code computes")
    features, _ = prepare_features(frame, artifact["medians"], now)
    model = artifact["model"]
    anomaly = -model.decision_function(features)
    scale = artifact["calibration_high"] - artifact["calibration_low"]
    risk = np.clip((anomaly - artifact["calibration_low"]) / scale, 0.0, 1.0)
    return risk, model.predict(features) == -1, anomaly


def default_threshold(artifact: dict) -> float:
    """The risk at the Isolation Forest's own decision boundary."""
    scale = artifact["calibration_high"] - artifact["calibration_low"]
    return float(np.clip(-artifact["calibration_low"] / scale, 0.0, 1.0))

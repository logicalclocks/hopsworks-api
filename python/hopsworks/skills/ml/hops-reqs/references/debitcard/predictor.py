"""KServe predictor for card_fraud_model: scores cards from their online features.

Request: {"inputs": [["<card_number>"], ...]}, one card per row.
Reply: {"predictions": [{"card_number", "risk", "fraud", "anomaly_score", "has_window", "has_baseline"}, ...]}.
A card's live window and baseline come from the feature view card_fraud_fv;
a card with no window or no baseline is scored on the training medians.
"""

import os
import sys

import joblib
import pandas as pd

import hopsworks

sys.path.insert(0, os.environ["MODEL_FILES_PATH"])
import model as fraud_model  # noqa: E402  (shipped with the model's files)


class Predict:
    def __init__(self):
        self.artifact = joblib.load(
            os.path.join(os.environ["MODEL_FILES_PATH"], fraud_model.MODEL_FILE)
        )
        self.threshold = fraud_model.default_threshold(self.artifact)
        fs = hopsworks.login().get_feature_store()
        self.fv = fs.get_feature_view(fraud_model.FEATURE_VIEW, version=1)
        self.fv.init_serving()

    def predict(self, inputs):
        cards = [str(row[0] if isinstance(row, (list, tuple)) else row) for row in inputs]
        frame = self.fv.get_feature_vectors(
            entry=[{"card_number": card} for card in cards],
            return_type="pandas",
            allow_missing=True,
        )
        frame = pd.DataFrame(frame).reset_index(drop=True)
        risk, _, anomaly = fraud_model.score(self.artifact, frame)
        window = frame.reindex(columns=["txn_count"]).iloc[:, 0].notna()
        baseline = frame.reindex(columns=["avg_daily_amount_eur_30d"]).iloc[:, 0].notna()
        return [
            {
                "card_number": card,
                "risk": round(float(r), 4),
                "fraud": bool(r >= self.threshold),
                "anomaly_score": round(float(a), 4),
                "has_window": bool(w),
                "has_baseline": bool(b),
            }
            for card, r, a, w, b in zip(cards, risk, anomaly, window, baseline, strict=True)
        ]

# ruff: noqa: INP001
"""The fraud example's deployment: scores one transaction with the registered pipeline.

A request is `{"inputs": [[cc_num, amount, merchant_id, ip_address, card_present, t_id]]}`;
the reply's `predictions` is `[true]` for fraud. The card's and the merchant's
features come from the online store, the sliding-window aggregates of
`cc_trans_aggs_fg` among them, and the transaction's own amount, IP address and
card presence are request parameters, which the feature view's on-demand
transformation turns into the distance from the card's previous transaction.
Each scored vector is logged to the feature view's logging tables.
"""

import os
from datetime import datetime

import hopsworks
import joblib


class Predict:
    def __init__(self, async_logger):
        project = hopsworks.login()
        model = project.get_model_registry().get_best_model(
            name="cc_fraud_xgboost_model", metric="f1_score", direction="max"
        )
        self.feature_view = model.get_feature_view()
        self.feature_view.init_feature_logger(async_logger)
        self.pipeline = joblib.load(
            os.environ["MODEL_FILES_PATH"] + "/cc_fraud_pipeline.pkl"
        )

    def predict(self, inputs):
        cc_num, amount, merchant_id, ip_address, card_present, t_id = inputs[0]
        keys = {"cc_num": cc_num, "merchant_id": merchant_id}
        features = self.feature_view.get_feature_vector(
            entry=keys,
            request_parameters={
                "t_id": t_id,
                "amount": amount,
                "ip_address": ip_address,
                "card_present": card_present,
            },
            return_type="pandas",
            allow_missing=True,
        )
        predictions = [bool(p) for p in self.pipeline.predict(features)]
        self.feature_view.log(
            features,
            predictions=predictions,
            serving_keys=[keys],
            event_time=[datetime.now()],
        )
        return predictions

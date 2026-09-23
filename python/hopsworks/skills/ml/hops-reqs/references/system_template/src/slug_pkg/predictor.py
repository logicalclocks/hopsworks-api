"""The realtime predictor: the default feature-view lookup, scored as the harness scores.

Deployed with the model it serves, so the default predictor still looks up the
feature vector, applies the feature view's transformations and validates the
request against the deployment schema:

    model.deploy(name="<alnum>", script_file="predictor.py", default_predictor=True)

A classifier returns the positive-class probability, the number evaluate.py,
the batch inference pipeline and the parity test compute; the default would
return class labels, and the parity test would fail on every entity.
"""

from __future__ import annotations

from typing import Any

from hsml.default_predictor import DefaultPredict


class Predict(DefaultPredict):
    """DefaultPredict, returning scores for classifiers."""

    def model_predict(self, feature_vectors: Any) -> Any:
        """Score the transformed vectors with the model, in its input column order."""
        if self.model is None or not hasattr(self.model, "predict_proba"):
            return super().model_predict(feature_vectors)
        x = feature_vectors[self.model_input_columns]
        return self.model.predict_proba(x)[:, 1].tolist()

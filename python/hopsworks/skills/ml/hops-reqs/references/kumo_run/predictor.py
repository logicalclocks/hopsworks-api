# ruff: noqa: INP001
"""Hopsworks predictor for NVIDIA Kumo Tabular, a pretrained in-context tabular classifier.

Every request carries a small labelled context table and the unlabelled query rows to
classify; the model reads the context as its training set, so nothing is trained or
looked up. Request (KServe v1, one or more instances):

    {"instances": [{
        "context": [{"f1": 1.2, "f2": "a", "label": "x"}, ...],   # labelled rows
        "query":   [{"f1": 0.7, "f2": "b"}, ...],                 # rows to classify
        "target": "label",                                        # the context's label column
        "num_estimators": 1                                       # optional, default 1
    }]}

Response, per instance: `classes`, `probabilities` (one row per query row, in the order
of `classes`), `labels` (the most probable class of each row) and `seconds`, the
model's own time.

PyTorch runs as many threads as the pod's CPU limit allows (TORCH_NUM_THREADS overrides
it): by default it starts one per node core, which oversubscribes the limit.
"""

import contextlib
import glob
import logging
import os
import time
from pathlib import Path

import pandas as pd
import sdm
import torch
from sdm import Task
from sdm.models import KumoTabular

logger = logging.getLogger("kumo_tabular")
logging.basicConfig(level=logging.INFO)


def cpu_limit() -> int:
    """The pod's CPU limit in whole cores (cgroup v2 cpu.max), at least one; one when unlimited."""
    with contextlib.suppress(OSError, ValueError):
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
        if quota != "max":
            return max(1, int(int(quota) / int(period)))
    return 1


def checkpoint():
    """The registered `<size>/classifier.pt`; the model's files mount under MODEL_FILES_PATH."""
    hits = glob.glob(
        f"{os.environ.get('MODEL_FILES_PATH', '/mnt/models')}/**/classifier.pt",
        recursive=True,
    )
    if not hits:
        raise FileNotFoundError("classifier.pt is not among the model's files")
    return hits[0]


class Predict:
    def __init__(self):
        torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", cpu_limit())))
        torch.set_grad_enabled(False)
        started = time.perf_counter()
        path = checkpoint()
        # The checkpoint's directory is its size (small, medium or large), as in
        # nvidia/Kumo-Tabular; the module must be built at that size to take it.
        size = Path(path).parent.name
        # The module is built on the meta device and the checkpoint's tensors assigned to
        # it: one copy of the weights in memory, and no download from Hugging Face, which
        # KumoTabular's own pretrained loader would do.
        self.model = KumoTabular(
            task=Task.classification, size=size, pretrained=False, device="meta"
        )
        state = torch.load(path, map_location="cpu", weights_only=True)
        self.model.models[Task.classification].load_state_dict(state, assign=True)
        self.model.eval()
        logger.info(
            "Kumo Tabular %s ready in %.1fs, %d threads",
            size,
            time.perf_counter() - started,
            torch.get_num_threads(),
        )

    def _classify(self, instance):
        for key in ("context", "query", "target"):
            if key not in instance:
                raise ValueError(f"each instance needs '{key}'")
        target = instance["target"]
        context = pd.DataFrame(instance["context"])
        query = pd.DataFrame(instance["query"])
        if target not in context.columns or len(context) < 2 or query.empty:
            raise ValueError(
                "the context needs two or more rows with the target column, the query one or more"
            )
        started = time.perf_counter()

        features = [c for c in context.columns if c != target]
        query = query.reindex(columns=features)  # a missing column is a missing value
        stypes = sdm.infer_stypes(context, overrides={target: "categorical"}, unsupported="drop")
        feature_stypes = {k: v for k, v in stypes.items() if k != target}
        # The query's dtypes follow the context's, so the column encoders agree.
        for column in feature_stypes:
            if query[column].dtype != context[column].dtype:
                with contextlib.suppress(TypeError, ValueError):
                    query[column] = query[column].astype(context[column].dtype)
        context[target] = context[target].astype(str)

        with torch.inference_mode():
            out = self.model(
                x_context=sdm.TableTensor.from_pandas(
                    context[list(feature_stypes)], stypes=feature_stypes, device="cpu"
                ),
                y_context=sdm.TableTensor.from_pandas(
                    context[[target]], stypes={target: "categorical"}, device="cpu"
                ),
                x_query=sdm.TableTensor.from_pandas(
                    query[list(feature_stypes)], stypes=feature_stypes, device="cpu"
                ),
                num_estimators=int(instance.get("num_estimators", 1)),
            )
        probabilities = out.to_pandas()
        classes = [str(c) for c in probabilities.columns]
        matrix = probabilities.to_numpy().astype(float)
        return {
            "classes": classes,
            "probabilities": [[round(float(p), 6) for p in row] for row in matrix],
            "labels": [classes[i] for i in matrix.argmax(axis=1)],
            "seconds": round(time.perf_counter() - started, 4),
        }

    def predict(self, inputs):
        if not isinstance(inputs, list):
            raise ValueError("the request carries a list of instances")
        return {"predictions": [self._classify(instance) for instance in inputs]}

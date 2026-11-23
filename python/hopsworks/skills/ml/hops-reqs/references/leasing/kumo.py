# ruff: noqa: INP001
"""NVIDIA Kumo Tabular, the pretrained in-context classifier, run inside the app.

Kumo Tabular reads a small labelled table, the context, as its training set and
classifies the query rows against it, so nothing is trained. The app's context is
the class-balanced sample of transformed training rows the training job stores with
the XGBoost model, so both models decide from the same features. The checkpoint is
the one register_kumo.py registered; its directory names its size (medium).
"""

from __future__ import annotations

import contextlib
import glob
import os
import time
from pathlib import Path

import pandas as pd


def cpu_limit() -> int:
    """The pod's CPU limit in whole cores (cgroup v2 cpu.max), at least one; one when unlimited."""
    with contextlib.suppress(OSError, ValueError):
        quota, period = (
            Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").split()
        )
        if quota != "max":
            return max(1, int(int(quota) / int(period)))
    return 1


class KumoTabular:
    """The classifier, loaded once from the registered checkpoint under `directory`."""

    def __init__(self, directory: str):
        import torch
        from sdm import Task
        from sdm.models import KumoTabular as Model

        # PyTorch starts one thread per node core by default, which oversubscribes the pod's limit.
        torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", cpu_limit())))
        torch.set_grad_enabled(False)
        hits = glob.glob(f"{directory}/**/classifier.pt", recursive=True)
        if not hits:
            raise FileNotFoundError(f"classifier.pt is not under {directory}")
        self.size = Path(hits[0]).parent.name
        # Built on the meta device and given the checkpoint's tensors: one copy of the
        # weights, and no download from Hugging Face.
        self.model = Model(
            task=Task.classification, size=self.size, pretrained=False, device="meta"
        )
        state = torch.load(hits[0], map_location="cpu", weights_only=True)
        self.model.models[Task.classification].load_state_dict(state, assign=True)
        self.model.eval()

    def probabilities(
        self, context: pd.DataFrame, query: pd.DataFrame, target: str
    ) -> tuple[pd.Series, float]:
        """The probability of class 1 for each query row, and the model's own seconds."""
        import sdm
        import torch

        started = time.perf_counter()
        features = [c for c in context.columns if c != target]
        query = query.reindex(columns=features)
        stypes = sdm.infer_stypes(
            context, overrides={target: "categorical"}, unsupported="drop"
        )
        feature_stypes = {k: v for k, v in stypes.items() if k != target}
        for column in feature_stypes:
            if query[column].dtype != context[column].dtype:
                with contextlib.suppress(TypeError, ValueError):
                    query[column] = query[column].astype(context[column].dtype)
        context = context.assign(**{target: context[target].astype(str)})
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
                num_estimators=1,
            )
        probabilities = out.to_pandas()
        probabilities.columns = [str(c) for c in probabilities.columns]
        leased = (
            probabilities["1"]
            if "1" in probabilities.columns
            else pd.Series(0.0, index=probabilities.index)
        )
        return leased.reset_index(drop=True), time.perf_counter() - started

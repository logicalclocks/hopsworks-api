#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

from hopsworks_common import util
from hsml.constants import INFERENCE_LOGGER, MODEL, PREDICTOR
from hsml.inference_batcher import InferenceBatcher
from hsml.inference_logger import InferenceLogger
from hsml.predictor import Predictor


class Predictor(Predictor):
    """Configuration for a predictor running a model on NVIDIA Triton Inference Server."""

    def __init__(self, triton_image_tag: str | None = None, **kwargs):
        if kwargs.get("script_file") is not None:
            raise ValueError(
                "Predictor scripts are not supported in deployments for Triton models"
            )
        if kwargs.get("transformer") is not None:
            raise ValueError(
                "Transformers are not supported in deployments for Triton models"
            )
        inference_logger = util._get_obj_from_json(
            kwargs.get("inference_logger"), InferenceLogger
        )
        if (
            inference_logger is not None
            and inference_logger.mode != INFERENCE_LOGGER.MODE_NONE
        ):
            raise ValueError(
                "Inference logging is not supported in deployments for Triton models"
            )
        inference_batcher = util._get_obj_from_json(
            kwargs.get("inference_batcher"), InferenceBatcher
        )
        if inference_batcher is not None and inference_batcher.enabled:
            raise ValueError(
                "Request batching is not supported in deployments for Triton models; "
                "configure dynamic batching in config.pbtxt instead"
            )

        kwargs["model_framework"] = MODEL.FRAMEWORK_TRITON
        kwargs["model_server"] = PREDICTOR.MODEL_SERVER_TRITON
        kwargs["triton_image_tag"] = triton_image_tag

        super().__init__(**kwargs)

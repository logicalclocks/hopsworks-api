#
#   Copyright 2024 Hopsworks AB
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

from hsml.constants import MODEL, PREDICTOR
from hsml.predictor import Predictor


class Predictor(Predictor):
    """Configuration for a predictor running with the vLLM backend."""

    def __init__(
        self,
        vllm_variant: str | None = None,
        vllm_image_tag: str | None = None,
        **kwargs,
    ):
        if vllm_variant is None:
            vllm_variant = PREDICTOR.VLLM_VARIANT_VLLM
        else:
            valid_variants = {PREDICTOR.VLLM_VARIANT_VLLM, PREDICTOR.VLLM_VARIANT_OMNI}
            if vllm_variant not in valid_variants:
                raise ValueError(
                    f"vLLM variant '{vllm_variant}' is not valid. Possible values are {sorted(valid_variants)}"
                )

        kwargs["model_framework"] = MODEL.FRAMEWORK_LLM
        kwargs["model_server"] = PREDICTOR.MODEL_SERVER_VLLM
        kwargs["vllm_variant"] = vllm_variant
        kwargs["vllm_image_tag"] = vllm_image_tag

        super().__init__(**kwargs)

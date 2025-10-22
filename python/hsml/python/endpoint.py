#
#   Copyright 2025 Hopsworks AB
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

from hsml.constants import PREDICTOR
from hsml.predictor import Predictor


class Endpoint(Predictor):
    """Configuration for an endpoint running a python script."""

    def __init__(self, name: str, script_file: str, **kwargs):
        kwargs["model_server"] = PREDICTOR.MODEL_SERVER_PYTHON

        if name is None:
            raise ValueError("A deployment name must be provided")
        if script_file is None:
            raise ValueError(
                "Python scripts are required in deployments without models"
            )

        super().__init__(name=name, script_file=script_file, **kwargs)

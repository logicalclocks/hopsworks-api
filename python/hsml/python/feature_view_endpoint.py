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

from hsml.constants import PREDICTOR
from hsml.deployment.predictor import Predictor


class FeatureViewEndpoint(Predictor):
    """Predictor of a feature view deployment: the default predictor with no model.

    Serialises to the backend as a plain Python endpoint; the feature view
    identity lives in the predictor env vars and the schema.
    """

    def __init__(self, name: str, **kwargs):
        kwargs["model_server"] = PREDICTOR.MODEL_SERVER_PYTHON
        kwargs["default_predictor"] = True
        if name is None:
            raise ValueError("A deployment name must be provided")
        super().__init__(name=name, **kwargs)

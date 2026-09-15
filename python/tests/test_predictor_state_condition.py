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

import copy

import humps
from hsml import predictor_state_condition


class TestPredictorStateCondition:
    # from response json

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]["condition"]
        json_camelized = humps.camelize(json)  # as returned by the backend

        # Act
        psc = predictor_state_condition.PredictorStateCondition.from_response_json(
            json_camelized
        )

        # Assert
        assert isinstance(psc, predictor_state_condition.PredictorStateCondition)
        assert psc.type == json["type"]
        assert psc.status == json["status"]
        assert psc.reason == json["reason"]

    # constructor

    def test_constructor(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]["condition"]

        # Act
        psc = predictor_state_condition.PredictorStateCondition(
            type=json["type"], status=json["status"], reason=json["reason"]
        )

        # Assert
        assert isinstance(psc, predictor_state_condition.PredictorStateCondition)
        assert psc.type == json["type"]
        assert psc.status == json["status"]
        assert psc.reason == json["reason"]

    # extract fields from json

    def test_extract_fields_from_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]["condition"]

        # Act
        kwargs = (
            predictor_state_condition.PredictorStateCondition.extract_fields_from_json(
                copy.deepcopy(json)
            )
        )

        # Assert
        assert kwargs["type"] == json["type"]
        assert kwargs["status"] == json["status"]
        assert kwargs["reason"] == json["reason"]

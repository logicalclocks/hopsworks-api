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
from hsml import predictor_state, predictor_state_condition


class TestPredictorState:
    # from response json

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]
        json_camelized = humps.camelize(json)  # as returned by the backend

        # Act
        ps = predictor_state.PredictorState.from_response_json(json_camelized)

        # Assert
        assert isinstance(ps, predictor_state.PredictorState)
        assert ps.available_predictor_instances == json["available_instances"]
        assert (
            ps.available_transformer_instances
            == json["available_transformer_instances"]
        )
        assert ps.hopsworks_inference_path == json["hopsworks_inference_path"]
        assert ps.model_server_inference_path == json["model_server_inference_path"]
        assert ps.internal_port == json["internal_port"]
        assert ps.revision == json["revision"]
        assert ps.deployed == json["deployed"]
        assert isinstance(
            ps.condition, predictor_state_condition.PredictorStateCondition
        )
        assert ps.condition.status == json["condition"]["status"]
        assert ps.status == json["status"]

    # constructor

    def test_constructor(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]

        # Act
        ps = predictor_state.PredictorState(
            available_predictor_instances=json["available_instances"],
            available_transformer_instances=json["available_transformer_instances"],
            hopsworks_inference_path=json["hopsworks_inference_path"],
            model_server_inference_path=json["model_server_inference_path"],
            internal_port=json["internal_port"],
            revision=json["revision"],
            deployed=json["deployed"],
            condition=predictor_state_condition.PredictorStateCondition(
                **copy.deepcopy(json["condition"])
            ),
            status=json["status"],
        )

        # Assert
        assert isinstance(ps, predictor_state.PredictorState)
        assert ps.available_predictor_instances == json["available_instances"]
        assert (
            ps.available_transformer_instances
            == json["available_transformer_instances"]
        )
        assert ps.hopsworks_inference_path == json["hopsworks_inference_path"]
        assert ps.model_server_inference_path == json["model_server_inference_path"]
        assert ps.internal_port == json["internal_port"]
        assert ps.revision == json["revision"]
        assert ps.deployed == json["deployed"]
        assert isinstance(
            ps.condition, predictor_state_condition.PredictorStateCondition
        )
        assert ps.condition.status == json["condition"]["status"]
        assert ps.status == json["status"]

    # extract fields from json

    def test_extract_fields_from_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_predictor_state"][
            "response"
        ]

        # Act
        (
            ai,
            ati,
            hip,
            msip,
            ipt,
            r,
            d,
            c,
            s,
        ) = predictor_state.PredictorState.extract_fields_from_json(copy.deepcopy(json))

        # Assert
        assert ai == json["available_instances"]
        assert ati == json["available_transformer_instances"]
        assert hip == json["hopsworks_inference_path"]
        assert msip == json["model_server_inference_path"]
        assert ipt == json["internal_port"]
        assert r == json["revision"]
        assert d == json["deployed"]
        assert isinstance(c, predictor_state_condition.PredictorStateCondition)
        assert c.status == json["condition"]["status"]
        assert s == json["status"]

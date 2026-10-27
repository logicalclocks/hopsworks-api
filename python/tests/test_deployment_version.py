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

import json

from hsml.deployment_version import DeploymentVersion
from hsml.resources import PredictorResources
from hsml.scaling_config import PredictorScalingConfig


class TestDeploymentVersion:
    def test_from_response_json_list(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_version"]["get_versions"]["response"]

        versions = DeploymentVersion.from_response_json(json_dict)

        assert [v.version for v in versions] == [2, 1]
        assert versions[0].active is True
        assert versions[1].active is False
        assert versions[0].created_by == "Ada Lovelace"
        assert versions[0].predictor == "predictor.py"
        assert versions[0].transformer == "transformer.py"
        assert versions[0].predictor_env_vars == ["A=1"]
        assert versions[0].predictor_environment_id == 3
        assert versions[0].predictor_resources.requests.cores == 1
        assert versions[1].updated == "2026-09-28T12:00:00Z"
        assert versions[1].transformer is None

    def test_from_response_json_single(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_version"]["get_version"]["response"]

        version = DeploymentVersion.from_response_json(json_dict)

        assert version.version == 3
        assert version.model_name == "m"
        assert version.model_version == 1

    def test_unknown_keys_are_ignored(self):
        version = DeploymentVersion.from_response_json(
            {"version": 1, "somethingNew": 1}
        )

        assert version.version == 1

    def test_json_round_trip(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_version"]["get_version"]["response"]
        version = DeploymentVersion.from_response_json(json_dict)

        again = DeploymentVersion.from_response_json(json.loads(version.json()))

        assert again.to_dict() == version.to_dict()

    def test_nested_fields_are_sdk_objects(self, backend_fixtures):
        json_dict = backend_fixtures["deployment_version"]["get_versions"]["response"]

        version = DeploymentVersion.from_response_json(json_dict)[0]

        assert isinstance(version.predictor_resources, PredictorResources)
        assert version.predictor_resources.requests.cores == 1
        assert isinstance(version.predictor_scaling_config, PredictorScalingConfig)
        assert version.predictor_scaling_config.min_instances == 1
        assert version.to_dict()["predictorScalingConfig"]["minInstances"] == 1

    def test_environment_names_are_exposed(self):
        version = DeploymentVersion.from_response_json(
            {
                "version": 1,
                "predictorEnvironment": {"name": "py-a"},
                "transformerEnvironment": {"name": "py-b"},
            }
        )

        assert version.predictor_environment == "py-a"
        assert version.transformer_environment == "py-b"
        assert version.to_dict()["predictorEnvironment"] == {"name": "py-a"}

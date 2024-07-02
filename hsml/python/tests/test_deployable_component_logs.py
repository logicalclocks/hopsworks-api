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

import datetime

import humps
from hsml import deployable_component_logs


class TestDeployableComponentLogs:
    # from response json

    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_component_logs_single"][
            "response"
        ]
        json_camelized = humps.camelize(json)
        mocker_from_json = mocker.patch(
            "hsml.deployable_component_logs.DeployableComponentLogs.from_json",
            return_value=None,
        )

        # Act
        dc_logs = deployable_component_logs.DeployableComponentLogs.from_response_json(
            json_camelized
        )

        # Assert
        assert isinstance(dc_logs, list)
        assert len(dc_logs) == 1
        mocker_from_json.assert_called_once_with(json[0])

    def test_from_response_json_list(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_component_logs_list"][
            "response"
        ]
        json_camelized = humps.camelize(json)
        mocker_from_json = mocker.patch(
            "hsml.deployable_component_logs.DeployableComponentLogs.from_json",
            return_value=None,
        )

        # Act
        dc_logs = deployable_component_logs.DeployableComponentLogs.from_response_json(
            json_camelized
        )

        # Assert
        assert isinstance(dc_logs, list)
        assert len(dc_logs) == len(json_camelized)
        assert mocker_from_json.call_count == len(json_camelized)

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_component_logs_empty"][
            "response"
        ]
        json_camelized = humps.camelize(json)
        mocker_from_json = mocker.patch(
            "hsml.deployable_component_logs.DeployableComponentLogs.from_json",
            return_value=None,
        )

        # Act
        dc_logs = deployable_component_logs.DeployableComponentLogs.from_response_json(
            json_camelized
        )

        # Assert
        assert isinstance(dc_logs, list)
        assert len(dc_logs) == 0
        mocker_from_json.assert_not_called()

    # constructor

    def test_constructor(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["predictor"]["get_deployment_component_logs_single"][
            "response"
        ]
        instance_name = json[0]["instance_name"]
        content = json[0]["content"]
        now = datetime.datetime.now()

        # Act
        dcl = deployable_component_logs.DeployableComponentLogs(
            instance_name=instance_name, content=content
        )

        # Assert
        assert dcl.instance_name == instance_name
        assert dcl.content == content
        assert (dcl.created_at >= now) and (
            dcl.created_at < (now + datetime.timedelta(seconds=1))
        )

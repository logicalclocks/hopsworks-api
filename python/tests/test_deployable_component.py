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

from hsml import deployable_component, inference_batcher


class TestDeployableComponent:
    # from response json

    def test_from_response_json(self, mocker):
        # Arrange
        json = {"test": "test"}
        mock_from_json = mocker.patch(
            "hsml.deployable_component.DeployableComponent.from_json",
            return_value="from_json_result",
        )

        # Act
        result = deployable_component.DeployableComponent.from_response_json(json)

        # Assert
        assert result == "from_json_result"
        mock_from_json.assert_called_once_with(json)

    # constructor

    def test_constructor_default(self, mocker):
        # Arrange
        mock_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=None
        )
        mock_ib_init = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.__init__", return_value=None
        )

        class DeployableComponentChild(deployable_component.DeployableComponent):
            def from_json():
                pass

            def update_from_response_json():
                pass

            def to_dict():
                pass

        # Act
        dc = DeployableComponentChild()

        # Assert
        assert dc.script_file is None
        assert dc.resources is None
        mock_get_obj_from_json.assert_called_once_with(
            None, inference_batcher.InferenceBatcher
        )
        mock_ib_init.assert_called_once()

    def test_constructor_with_params(self, mocker):
        # Arrange
        script_file = "script_file"
        resources = {}
        inf_batcher = inference_batcher.InferenceBatcher()
        mock_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=inf_batcher
        )
        mock_ib_init = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.__init__", return_value=None
        )

        class DeployableComponentChild(deployable_component.DeployableComponent):
            def from_json():
                pass

            def update_from_response_json():
                pass

            def to_dict():
                pass

        # Act
        dc = DeployableComponentChild(
            script_file=script_file,
            resources=resources,
            inference_batcher=inf_batcher,
        )

        # Assert
        assert dc.script_file == script_file
        assert dc.resources == resources
        mock_get_obj_from_json.assert_called_once_with(
            inf_batcher, inference_batcher.InferenceBatcher
        )
        assert dc.inference_batcher == inf_batcher
        mock_ib_init.assert_not_called()

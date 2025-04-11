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

from hsml import resources
from hsml.constants import RESOURCES
from mock import call


SERVING_RESOURCE_LIMITS = {"cores": 2, "memory": 1024, "gpus": 0}


class TestResources:
    # Resources

    def test_from_response_json_cpus(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_only_cores"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores == json["cores"]
        assert r.memory is None
        assert r.gpus is None

    def test_from_response_json_memory(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_only_memory"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores is None
        assert r.memory is json["memory"]
        assert r.gpus is None

    def test_from_response_json_gpus(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_only_gpus"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores is None
        assert r.memory is None
        assert r.gpus == json["gpus"]

    def test_from_response_json_cores_and_memory(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_cores_and_memory"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores == json["cores"]
        assert r.memory == json["memory"]
        assert r.gpus is None

    def test_from_response_json_cores_and_gpus(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_cores_and_gpus"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores == json["cores"]
        assert r.memory is None
        assert r.gpus == json["gpus"]

    def test_from_response_json_memory_and_gpus(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_memory_and_gpus"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores is None
        assert r.memory == json["memory"]
        assert r.gpus == json["gpus"]

    def test_from_response_json_cores_memory_and_gpus(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"]["get_cores_memory_and_gpus"]["response"]

        # Act
        r = resources.Resources.from_response_json(json)

        # Assert
        assert r.cores == json["cores"]
        assert r.memory == json["memory"]
        assert r.gpus == json["gpus"]

    # ComponentResources

    # - from response json

    def test_from_response_json_component_resources(self, mocker):
        # Arrange
        res = {"something": "here"}
        json_decamelized = {"key": "value"}
        mock_humps_decamelize = mocker.patch(
            "humps.decamelize", return_value=json_decamelized
        )
        mock_from_json = mocker.patch(
            "hsml.resources.ComponentResources.from_json",
            return_value="from_json_result",
        )

        # Act
        result = resources.ComponentResources.from_response_json(res)

        # Assert
        assert result == "from_json_result"
        mock_humps_decamelize.assert_called_once_with(res)
        mock_from_json.assert_called_once_with(json_decamelized)

    # - constructor

    def test_constructor_component_resources_default(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_requests_and_limits"
        ]["response"]
        mock_fill_missing_resources = mocker.patch(
            "hsml.resources.ComponentResources._fill_missing_resources"
        )
        mock_resources_init = mocker.spy(resources.Resources, "__init__")

        # Act
        pr = resources.PredictorResources(num_instances=json["num_instances"])

        # Assert
        assert pr.num_instances == json["num_instances"]
        assert mock_fill_missing_resources.call_count == 2
        assert (
            mock_fill_missing_resources.call_args_list[0][0][1] == RESOURCES.MIN_CORES
        )
        assert (
            mock_fill_missing_resources.call_args_list[0][0][2] == RESOURCES.MIN_MEMORY
        )
        assert mock_fill_missing_resources.call_args_list[0][0][3] == RESOURCES.GPUS
        assert mock_fill_missing_resources.call_args_list[1][0][1] == RESOURCES.MAX_CORES
        assert mock_fill_missing_resources.call_args_list[1][0][2] == RESOURCES.MAX_MEMORY
        assert mock_fill_missing_resources.call_args_list[1][0][3] == RESOURCES.GPUS
        expected_calls = [
            call(pr.requests, RESOURCES.MIN_CORES, RESOURCES.MIN_MEMORY, RESOURCES.GPUS),
            call(pr.limits, RESOURCES.MAX_CORES, RESOURCES.MAX_MEMORY, RESOURCES.GPUS),
        ]
        mock_resources_init.assert_has_calls(expected_calls)

    def test_constructor_component_resources(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_requests_and_limits"
        ]["response"]

        requests_object = resources.Resources.from_json(json["requests"])
        limits_object = resources.Resources.from_json(json["limits"])
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json",
            side_effect=[requests_object, limits_object],
        )
        mock_fill_missing_resources = mocker.patch(
            "hsml.resources.ComponentResources._fill_missing_resources"
        )

        # Act
        pr = resources.PredictorResources(
            num_instances=json["num_instances"],
            requests=json["requests"],
            limits=json["limits"],
        )

        # Assert
        assert pr.num_instances == json["num_instances"]
        assert pr.requests == requests_object
        assert pr.limits == limits_object
        assert mock_fill_missing_resources.call_count == 2
        assert (
            mock_fill_missing_resources.call_args_list[0][0][1] == RESOURCES.MIN_CORES
        )
        assert (
            mock_fill_missing_resources.call_args_list[0][0][2] == RESOURCES.MIN_MEMORY
        )
        assert mock_fill_missing_resources.call_args_list[0][0][3] == RESOURCES.GPUS
        assert mock_fill_missing_resources.call_args_list[1][0][1] == RESOURCES.MAX_CORES
        assert mock_fill_missing_resources.call_args_list[1][0][2] == RESOURCES.MAX_MEMORY
        assert mock_fill_missing_resources.call_args_list[1][0][3] == requests_object.gpus
        assert mock_util_get_obj_from_json.call_count == 2
        expected_calls = [
            call(json["requests"], resources.Resources),
            call(json["limits"], resources.Resources),
        ]
        mock_util_get_obj_from_json.assert_has_calls(expected_calls)

    # - extract fields from json

    def test_extract_fields_from_json_component_resources_with_key(
        self, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["resources"][
            "get_component_resources_requested_instances_and_predictor_resources"
        ]["response"]
        copy_json = copy.deepcopy(json)
        resources.ComponentResources.RESOURCES_CONFIG_KEY = "predictor_resources"
        resources.ComponentResources.NUM_INSTANCES_KEY = "requested_instances"

        # Act
        kwargs = resources.ComponentResources.extract_fields_from_json(copy_json)

        # Assert
        assert kwargs["num_instances"] == json["requested_instances"]
        assert isinstance(kwargs["requests"], resources.Resources)
        assert (
            kwargs["requests"].cores == json["predictor_resources"]["requests"]["cores"]
        )
        assert (
            kwargs["requests"].memory
            == json["predictor_resources"]["requests"]["memory"]
        )
        assert (
            kwargs["requests"].gpus == json["predictor_resources"]["requests"]["gpus"]
        )
        assert isinstance(kwargs["limits"], resources.Resources)
        assert kwargs["limits"].cores == json["predictor_resources"]["limits"]["cores"]
        assert (
            kwargs["limits"].memory == json["predictor_resources"]["limits"]["memory"]
        )
        assert kwargs["limits"].gpus == json["predictor_resources"]["limits"]["gpus"]

    def test_extract_fields_from_json_component_resources(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["resources"][
            "get_component_resources_requested_instances_and_predictor_resources_alternative"
        ]["response"]
        copy_json = copy.deepcopy(json)
        resources.ComponentResources.RESOURCES_CONFIG_KEY = "predictor_resources"
        resources.ComponentResources.NUM_INSTANCES_KEY = "requested_instances"

        # Act
        kwargs = resources.ComponentResources.extract_fields_from_json(copy_json)

        # Assert
        assert kwargs["num_instances"] == json["num_instances"]
        assert isinstance(kwargs["requests"], resources.Resources)
        assert kwargs["requests"].cores == json["resources"]["requests"]["cores"]
        assert kwargs["requests"].memory == json["resources"]["requests"]["memory"]
        assert kwargs["requests"].gpus == json["resources"]["requests"]["gpus"]
        assert isinstance(kwargs["limits"], resources.Resources)
        assert kwargs["limits"].cores == json["resources"]["limits"]["cores"]
        assert kwargs["limits"].memory == json["resources"]["limits"]["memory"]
        assert kwargs["limits"].gpus == json["resources"]["limits"]["gpus"]

    def test_extract_fields_from_json_component_resources_flatten(
        self, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_requests_and_limits"
        ]["response"]
        copy_json = copy.deepcopy(json)
        resources.ComponentResources.RESOURCES_CONFIG_KEY = "predictor_resources"
        resources.ComponentResources.NUM_INSTANCES_KEY = "requested_instances"

        # Act
        kwargs = resources.ComponentResources.extract_fields_from_json(copy_json)

        # Assert
        assert kwargs["num_instances"] == json["num_instances"]
        assert isinstance(kwargs["requests"], resources.Resources)
        assert kwargs["requests"].cores == json["requests"]["cores"]
        assert kwargs["requests"].memory == json["requests"]["memory"]
        assert kwargs["requests"].gpus == json["requests"]["gpus"]
        assert isinstance(kwargs["limits"], resources.Resources)
        assert kwargs["limits"].cores == json["limits"]["cores"]
        assert kwargs["limits"].memory == json["limits"]["memory"]
        assert kwargs["limits"].gpus == json["limits"]["gpus"]

    # - fill missing dependencies

    def test_fill_missing_dependencies_none(self, mocker):
        # Arrange
        class MockResources:
            cores = None
            memory = None
            gpus = None

        mock_resource = MockResources()

        # Act
        resources.ComponentResources._fill_missing_resources(mock_resource, 10, 11, 12)

        # Assert
        assert mock_resource.cores == 10
        assert mock_resource.memory == 11
        assert mock_resource.gpus == 12

    def test_fill_missing_dependencies_all(self):
        # Arrange
        class MockResources:
            cores = 1
            memory = 2
            gpus = 3

        mock_resource = MockResources()

        # Act
        resources.ComponentResources._fill_missing_resources(mock_resource, 10, 11, 12)

        # Assert
        assert mock_resource.cores == 1
        assert mock_resource.memory == 2
        assert mock_resource.gpus == 3

    def test_fill_missing_dependencies_some(self):
        # Arrange
        class MockResources:
            cores = 1
            memory = None
            gpus = None

        mock_resource = MockResources()

        # Act
        resources.ComponentResources._fill_missing_resources(mock_resource, 10, 11, 12)

        # Assert
        assert mock_resource.cores == 1
        assert mock_resource.memory == 11
        assert mock_resource.gpus == 12

    # PredictorResources

    def test_from_response_json_predictor_resources(self, backend_fixtures):
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_requests_and_limits"
        ]["response"]

        # Act
        r = resources.PredictorResources.from_response_json(json)

        # Assert
        assert r.num_instances == json["num_instances"]
        assert r.requests.cores == json["requests"]["cores"]
        assert r.requests.memory == json["requests"]["memory"]
        assert r.requests.gpus == json["requests"]["gpus"]
        assert r.limits.cores == json["limits"]["cores"]
        assert r.limits.memory == json["limits"]["memory"]
        assert r.limits.gpus == json["limits"]["gpus"]

    def test_from_response_json_predictor_resources_specific_keys(
        self, backend_fixtures
    ):
        json = backend_fixtures["resources"][
            "get_component_resources_requested_instances_and_predictor_resources"
        ]["response"]

        # Act
        r = resources.PredictorResources.from_response_json(json)

        # Assert
        assert r.num_instances == json["requested_instances"]
        assert r.requests.cores == json["predictor_resources"]["requests"]["cores"]
        assert r.requests.memory == json["predictor_resources"]["requests"]["memory"]
        assert r.requests.gpus == json["predictor_resources"]["requests"]["gpus"]
        assert r.limits.cores == json["predictor_resources"]["limits"]["cores"]
        assert r.limits.memory == json["predictor_resources"]["limits"]["memory"]
        assert r.limits.gpus == json["predictor_resources"]["limits"]["gpus"]

    # TransformerResources

    def test_from_response_json_transformer_resources(self, backend_fixtures):
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_requests_and_limits"
        ]["response"]

        # Act
        r = resources.TransformerResources.from_response_json(json)

        # Assert
        assert r.num_instances == json["num_instances"]
        assert r.requests.cores == json["requests"]["cores"]
        assert r.requests.memory == json["requests"]["memory"]
        assert r.requests.gpus == json["requests"]["gpus"]
        assert r.limits.cores == json["limits"]["cores"]
        assert r.limits.memory == json["limits"]["memory"]
        assert r.limits.gpus == json["limits"]["gpus"]

    def test_from_response_json_transformer_resources_specific_keys(
        self, backend_fixtures
    ):
        json = backend_fixtures["resources"][
            "get_component_resources_requested_instances_and_transformer_resources"
        ]["response"]

        # Act
        r = resources.TransformerResources.from_response_json(json)

        # Assert
        assert r.num_instances == json["requested_transformer_instances"]
        assert r.requests.cores == json["transformer_resources"]["requests"]["cores"]
        assert r.requests.memory == json["transformer_resources"]["requests"]["memory"]
        assert r.requests.gpus == json["transformer_resources"]["requests"]["gpus"]
        assert r.limits.cores == json["transformer_resources"]["limits"]["cores"]
        assert r.limits.memory == json["transformer_resources"]["limits"]["memory"]
        assert r.limits.gpus == json["transformer_resources"]["limits"]["gpus"]

    def test_from_response_json_transformer_resources_default_limits(
        self, backend_fixtures
    ):
        json = backend_fixtures["resources"][
            "get_component_resources_num_instances_and_requests"
        ]["response"]

        # Act
        r = resources.TransformerResources.from_response_json(json)

        # Assert
        assert r.num_instances == json["num_instances"]
        assert r.requests.cores == json["requests"]["cores"]
        assert r.requests.memory == json["requests"]["memory"]
        assert r.requests.gpus == json["requests"]["gpus"]
        assert r.limits.cores == SERVING_RESOURCE_LIMITS["cores"]
        assert r.limits.memory == SERVING_RESOURCE_LIMITS["memory"]
        assert r.limits.gpus == SERVING_RESOURCE_LIMITS["gpus"]

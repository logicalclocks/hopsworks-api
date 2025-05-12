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

import pytest
from hsml import resources, transformer
from hsml.constants import RESOURCES


SERVING_NUM_INSTANCES_NO_LIMIT = [-1]
SERVING_NUM_INSTANCES_SCALE_TO_ZERO = [0]
SERVING_NUM_INSTANCES_ONE = [0]


class TestTransformer:
    # from response json

    def test_from_response_json_with_transformer_field(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_with_transformer"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["transformer"]

        tr_resources = json["transformer_resources"]
        assert (
            t.resources.num_instances == tr_resources["requested_transformer_instances"]
        )
        assert t.resources.requests.cores == tr_resources["requests"]["cores"]
        assert t.resources.requests.memory == tr_resources["requests"]["memory"]
        assert t.resources.requests.gpus == tr_resources["requests"]["gpus"]
        assert t.resources.limits.cores == tr_resources["limits"]["cores"]
        assert t.resources.limits.memory == tr_resources["limits"]["memory"]
        assert t.resources.limits.gpus == tr_resources["limits"]["gpus"]

    def test_from_response_json_with_script_file_field(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_transformer_with_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["script_file"]

        tr_resources = json["resources"]
        assert t.resources.num_instances == tr_resources["num_instances"]
        assert t.resources.requests.cores == tr_resources["requests"]["cores"]
        assert t.resources.requests.memory == tr_resources["requests"]["memory"]
        assert t.resources.requests.gpus == tr_resources["requests"]["gpus"]
        assert t.resources.limits.cores == tr_resources["limits"]["cores"]
        assert t.resources.limits.memory == tr_resources["limits"]["memory"]
        assert t.resources.limits.gpus == tr_resources["limits"]["gpus"]

    def test_from_response_json_empty(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_without_transformer"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert t is None

    def test_from_response_json_default_resources(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer.from_response_json(json)

        # Assert
        assert isinstance(t, transformer.Transformer)
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == RESOURCES.MIN_NUM_INSTANCES
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    # constructor

    def test_constructor_default_resources(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer(json["script_file"], resources=None)

        # Assert
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == RESOURCES.MIN_NUM_INSTANCES
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    def test_constructor_default_resources_when_scale_to_zero_is_required(
        self, mocker, backend_fixtures
    ):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        json = backend_fixtures["transformer"]["get_transformer_without_resources"][
            "response"
        ]

        # Act
        t = transformer.Transformer(json["script_file"], resources=None)

        # Assert
        assert t.script_file == json["script_file"]

        assert t.resources.num_instances == 0
        assert t.resources.requests.cores == RESOURCES.MIN_CORES
        assert t.resources.requests.memory == RESOURCES.MIN_MEMORY
        assert t.resources.requests.gpus == RESOURCES.GPUS
        assert t.resources.limits.cores == RESOURCES.MAX_CORES
        assert t.resources.limits.memory == RESOURCES.MAX_MEMORY
        assert t.resources.limits.gpus == RESOURCES.GPUS

    # validate resources

    def test_validate_resources_none(self):
        # Act
        res = transformer.Transformer._validate_resources(None)

        # Assert
        assert res is None

    def test_validate_resources_num_instances_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        tr = resources.TransformerResources(num_instances=0)

        # Act
        res = transformer.Transformer._validate_resources(tr)

        # Assert
        assert res == tr

    def test_validate_resources_num_instances_one_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )
        tr = resources.TransformerResources(num_instances=1)

        # Act
        res = transformer.Transformer._validate_resources(tr)

        # Assert
        assert res == tr

    def test_validate_resources_num_instances_one_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )
        tr = resources.TransformerResources(num_instances=1)

        # Act
        with pytest.raises(ValueError) as e_info:
            _ = transformer.Transformer._validate_resources(tr)

        # Assert
        assert "Scale-to-zero is required" in str(e_info.value)

    # default num instances

    def test_get_default_num_instances_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        num_instances = transformer.Transformer._get_default_num_instances()

        # Assert
        assert num_instances == RESOURCES.MIN_NUM_INSTANCES

    def test_get_default_num_instances_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        num_instances = transformer.Transformer._get_default_num_instances()

        # Assert
        assert num_instances == 0

    # default resources

    def test_get_default_resources_without_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=False
        )

        # Act
        res = transformer.Transformer._get_default_resources()

        # Assert
        assert isinstance(res, resources.TransformerResources)
        assert res.num_instances == RESOURCES.MIN_NUM_INSTANCES

    def test_get_default_resources_with_scale_to_zero(self, mocker):
        # Arrange
        self._mock_serving_variables(
            mocker, SERVING_NUM_INSTANCES_NO_LIMIT, force_scale_to_zero=True
        )

        # Act
        res = transformer.Transformer._get_default_resources()

        # Assert
        assert isinstance(res, resources.TransformerResources)
        assert res.num_instances == 0

    # extract fields from json

    def test_extract_fields_from_json(self, mocker, backend_fixtures):
        # Arrange
        self._mock_serving_variables(mocker, SERVING_NUM_INSTANCES_NO_LIMIT)
        json = backend_fixtures["transformer"]["get_deployment_with_transformer"][
            "response"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        sf, rc = transformer.Transformer.extract_fields_from_json(json_copy)

        # Assert
        assert sf == json["transformer"]
        assert isinstance(rc, resources.TransformerResources)

        tr_resources = json["transformer_resources"]
        assert rc.num_instances == tr_resources["requested_transformer_instances"]
        assert rc.requests.cores == tr_resources["requests"]["cores"]
        assert rc.requests.memory == tr_resources["requests"]["memory"]
        assert rc.requests.gpus == tr_resources["requests"]["gpus"]
        assert rc.limits.cores == tr_resources["limits"]["cores"]
        assert rc.limits.memory == tr_resources["limits"]["memory"]
        assert rc.limits.gpus == tr_resources["limits"]["gpus"]

    # auxiliary methods

    def _mock_serving_variables(self, mocker, num_instances, force_scale_to_zero=False):
        mocker.patch(
            "hopsworks_common.client.get_serving_num_instances_limits",
            return_value=num_instances,
        )
        mocker.patch(
            "hopsworks_common.client.is_scale_to_zero_required",
            return_value=force_scale_to_zero,
        )

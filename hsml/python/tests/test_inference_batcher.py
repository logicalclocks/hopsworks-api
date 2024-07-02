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
from hsml import inference_batcher
from hsml.constants import INFERENCE_BATCHER


class TestInferenceBatcher:
    # from response json

    def test_from_response_json_enabled(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ib_from_json = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.from_json"
        )

        # Act
        _ = inference_batcher.InferenceBatcher.from_response_json(json_camelized)

        # Assert
        mock_ib_from_json.assert_called_once_with(json)

    def test_from_response_json_enabled_with_config(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled_with_config"][
            "response"
        ]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_ib_from_json = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.from_json"
        )

        # Act
        _ = inference_batcher.InferenceBatcher.from_response_json(json_camelized)

        # Assert
        mock_ib_from_json.assert_called_once_with(json)

    # from json

    def test_from_json_enabled(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled"]["response"]
        mock_ib_extract_fields = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.extract_fields_from_json",
            return_value=json,
        )
        mock_ib_init = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.__init__", return_value=None
        )

        # Act
        _ = inference_batcher.InferenceBatcher.from_json(json)

        # Assert
        mock_ib_extract_fields.assert_called_once_with(json)
        mock_ib_init.assert_called_once_with(**json)

    def test_from_json_enabled_with_config(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled_with_config"][
            "response"
        ]
        mock_ib_extract_fields = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.extract_fields_from_json",
            return_value=json,
        )
        mock_ib_init = mocker.patch(
            "hsml.inference_batcher.InferenceBatcher.__init__", return_value=None
        )

        # Act
        _ = inference_batcher.InferenceBatcher.from_json(json)

        # Assert
        mock_ib_extract_fields.assert_called_once_with(json)
        mock_ib_init.assert_called_once_with(**json)

    # constructor

    def test_constructor_default(self):
        # Act
        ib = inference_batcher.InferenceBatcher()

        # Assert
        assert isinstance(ib, inference_batcher.InferenceBatcher)
        assert ib.enabled == INFERENCE_BATCHER.ENABLED
        assert ib.max_batch_size is None
        assert ib.max_latency is None
        assert ib.timeout is None

    def test_constructor_enabled(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled"]["response"]

        # Act
        ib = inference_batcher.InferenceBatcher(**json)

        # Assert
        assert isinstance(ib, inference_batcher.InferenceBatcher)
        assert ib.enabled == json["enabled"]
        assert ib.max_batch_size is None
        assert ib.max_latency is None
        assert ib.timeout is None

    def test_constructor_enabled_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled_with_config"][
            "response"
        ]

        # Act
        ib = inference_batcher.InferenceBatcher(**json)

        # Assert
        assert isinstance(ib, inference_batcher.InferenceBatcher)
        assert ib.enabled == json["enabled"]
        assert ib.max_batch_size == json["max_batch_size"]
        assert ib.max_latency == json["max_latency"]
        assert ib.timeout == json["timeout"]

    def test_constructor_disabled(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_disabled"]["response"]

        # Act
        ib = inference_batcher.InferenceBatcher(**json)

        # Assert
        assert isinstance(ib, inference_batcher.InferenceBatcher)
        assert ib.enabled == json["enabled"]
        assert ib.max_batch_size is None
        assert ib.max_latency is None
        assert ib.timeout is None

    def test_constructor_disabled_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_disabled_with_config"][
            "response"
        ]

        # Act
        ib = inference_batcher.InferenceBatcher(**json)

        # Assert
        assert isinstance(ib, inference_batcher.InferenceBatcher)
        assert ib.enabled == json["enabled"]
        assert ib.max_batch_size == json["max_batch_size"]
        assert ib.max_latency == json["max_latency"]
        assert ib.timeout == json["timeout"]

    # # extract fields from json

    def test_extract_fields_from_json_enabled(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled"]["response"]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_batcher.InferenceBatcher.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["enabled"] == json["enabled"]
        assert kwargs["max_batch_size"] is None
        assert kwargs["max_latency"] is None
        assert kwargs["timeout"] is None

    def test_extract_fields_from_json_enabled_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled_with_config"][
            "response"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_batcher.InferenceBatcher.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["enabled"] == json["enabled"]
        assert kwargs["max_batch_size"] == json["max_batch_size"]
        assert kwargs["max_latency"] == json["max_latency"]
        assert kwargs["timeout"] == json["timeout"]

    def test_extract_fields_from_json_enabled_nested(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled"]["response_nested"]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_batcher.InferenceBatcher.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["enabled"] == json["batching_configuration"]["enabled"]
        assert kwargs["max_batch_size"] is None
        assert kwargs["max_latency"] is None
        assert kwargs["timeout"] is None

    def test_extract_fields_from_json_enabled_with_config_nested(
        self, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["inference_batcher"]["get_enabled_with_config"][
            "response_nested"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_batcher.InferenceBatcher.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["enabled"] == json["batching_configuration"]["enabled"]
        assert (
            kwargs["max_batch_size"] == json["batching_configuration"]["max_batch_size"]
        )
        assert kwargs["max_latency"] == json["batching_configuration"]["max_latency"]
        assert kwargs["timeout"] == json["batching_configuration"]["timeout"]

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
import pytest
from hsml import inference_logger, kafka_topic
from hsml.constants import DEFAULT, INFERENCE_LOGGER


class TestInferenceLogger:
    # from response json

    def test_from_response_json_with_mode_only(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all"]["response"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_il_from_json = mocker.patch(
            "hsml.inference_logger.InferenceLogger.from_json"
        )

        # Act
        _ = inference_logger.InferenceLogger.from_response_json(json_camelized)

        # Assert
        mock_il_from_json.assert_called_once_with(json)

    def test_from_response_json_with_mode_and_kafka_topic(
        self, mocker, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "response"
        ]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_il_from_json = mocker.patch(
            "hsml.inference_logger.InferenceLogger.from_json"
        )

        # Act
        _ = inference_logger.InferenceLogger.from_response_json(json_camelized)

        # Assert
        mock_il_from_json.assert_called_once_with(json)

    # from json

    def test_from_json_with_mode_all(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all"]["response"]
        mock_il_extract_fields = mocker.patch(
            "hsml.inference_logger.InferenceLogger.extract_fields_from_json",
            return_value=json,
        )
        mock_il_init = mocker.patch(
            "hsml.inference_logger.InferenceLogger.__init__", return_value=None
        )

        # Act
        _ = inference_logger.InferenceLogger.from_json(json)

        # Assert
        mock_il_extract_fields.assert_called_once_with(json)
        mock_il_init.assert_called_once_with(**json)

    def test_from_json_with_mode_all_and_kafka_topic(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "response"
        ]
        mock_il_extract_fields = mocker.patch(
            "hsml.inference_logger.InferenceLogger.extract_fields_from_json",
            return_value=json,
        )
        mock_il_init = mocker.patch(
            "hsml.inference_logger.InferenceLogger.__init__", return_value=None
        )

        # Act
        _ = inference_logger.InferenceLogger.from_json(json)

        # Assert
        mock_il_extract_fields.assert_called_once_with(json)
        mock_il_init.assert_called_once_with(**json)

    # constructor

    def test_constructor_default(self, mocker):
        # Arrange
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=INFERENCE_LOGGER.MODE_ALL,
        )
        default_kt = kafka_topic.KafkaTopic()
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=default_kt
        )

        # Act
        il = inference_logger.InferenceLogger()

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == INFERENCE_LOGGER.MODE_ALL
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == default_kt.name
        assert il.kafka_topic.num_replicas == default_kt.num_replicas
        assert il.kafka_topic.num_partitions == default_kt.num_partitions
        mock_util_get_obj_from_json.assert_called_once_with(
            DEFAULT, kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(
            INFERENCE_LOGGER.MODE_ALL, default_kt
        )

    def test_constructor_mode_all(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all"]["init_args"]
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        default_kt = kafka_topic.KafkaTopic()
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=default_kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == default_kt.name
        assert il.kafka_topic.num_replicas == default_kt.num_replicas
        assert il.kafka_topic.num_partitions == default_kt.num_partitions
        mock_util_get_obj_from_json.assert_called_once_with(
            DEFAULT, kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], default_kt)

    def test_constructor_mode_inputs(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_inputs"]["init_args"]
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        default_kt = kafka_topic.KafkaTopic()
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=default_kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == default_kt.name
        assert il.kafka_topic.num_replicas == default_kt.num_replicas
        assert il.kafka_topic.num_partitions == default_kt.num_partitions
        mock_util_get_obj_from_json.assert_called_once_with(
            DEFAULT, kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], default_kt)

    def test_constructor_mode_outputs(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_outputs"]["init_args"]
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        default_kt = kafka_topic.KafkaTopic()
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=default_kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == default_kt.name
        assert il.kafka_topic.num_replicas == default_kt.num_replicas
        assert il.kafka_topic.num_partitions == default_kt.num_partitions
        mock_util_get_obj_from_json.assert_called_once_with(
            DEFAULT, kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], default_kt)

    def test_constructor_mode_all_and_kafka_topic(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "init_args"
        ]
        json_copy = copy.deepcopy(json)
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        kt = kafka_topic.KafkaTopic(json["kafka_topic"]["name"])
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json_copy)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == kt.name
        assert il.kafka_topic.num_replicas is None
        assert il.kafka_topic.num_partitions is None
        mock_util_get_obj_from_json.assert_called_once_with(
            json["kafka_topic"], kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], kt)

    def test_constructor_mode_inputs_and_kafka_topic(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_inputs_with_kafka_topic"][
            "init_args"
        ]
        json_copy = copy.deepcopy(json)
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        kt = kafka_topic.KafkaTopic(json["kafka_topic"]["name"])
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json_copy)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == kt.name
        assert il.kafka_topic.num_replicas is None
        assert il.kafka_topic.num_partitions is None
        mock_util_get_obj_from_json.assert_called_once_with(
            json["kafka_topic"], kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], kt)

    def test_constructor_mode_outputs_and_kafka_topic(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "init_args"
        ]
        json_copy = copy.deepcopy(json)
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        kt = kafka_topic.KafkaTopic(json["kafka_topic"]["name"])
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json_copy)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == kt.name
        assert il.kafka_topic.num_replicas is None
        assert il.kafka_topic.num_partitions is None
        mock_util_get_obj_from_json.assert_called_once_with(
            json["kafka_topic"], kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], kt)

    def test_constructor_mode_none_and_kafka_topic(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_none_with_kafka_topic"][
            "init_args"
        ]
        json_copy = copy.deepcopy(json)
        mock_il_validate_mode = mocker.patch(
            "hsml.inference_logger.InferenceLogger._validate_mode",
            return_value=json["mode"],
        )
        kt = kafka_topic.KafkaTopic(json["kafka_topic"]["name"])
        mock_util_get_obj_from_json = mocker.patch(
            "hopsworks_common.util.get_obj_from_json", return_value=kt
        )

        # Act
        il = inference_logger.InferenceLogger(**json_copy)

        # Assert
        assert isinstance(il, inference_logger.InferenceLogger)
        assert il.mode == json["mode"]
        assert isinstance(il.kafka_topic, kafka_topic.KafkaTopic)
        assert il.kafka_topic.name == kt.name
        assert il.kafka_topic.num_replicas is None
        assert il.kafka_topic.num_partitions is None
        mock_util_get_obj_from_json.assert_called_once_with(
            json["kafka_topic"], kafka_topic.KafkaTopic
        )
        mock_il_validate_mode.assert_called_once_with(json["mode"], kt)

    # validate mode

    def test_validate_mode_none_and_kafka_topic_none(self):
        # Act
        mode = inference_logger.InferenceLogger._validate_mode(None, None)

        # Assert
        assert mode is None

    def test_validate_mode_all_and_kafka_topic_none(self):
        # Act
        mode = inference_logger.InferenceLogger._validate_mode(
            INFERENCE_LOGGER.MODE_ALL, None
        )

        # Assert
        assert mode is None

    def test_validate_mode_invalid_and_kafka_topic_none(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            _ = inference_logger.InferenceLogger._validate_mode("invalid", None)

        # Assert
        assert "is not valid" in str(e_info.value)

    def test_validate_mode_none_and_kafka_topic(self):
        # Act
        mode = inference_logger.InferenceLogger._validate_mode(
            None, kafka_topic.KafkaTopic()
        )

        # Assert
        assert mode == INFERENCE_LOGGER.MODE_NONE

    def test_validate_mode_all_and_kafka_topic(self):
        # Act
        mode = inference_logger.InferenceLogger._validate_mode(
            INFERENCE_LOGGER.MODE_ALL, kafka_topic.KafkaTopic()
        )

        # Assert
        assert mode == INFERENCE_LOGGER.MODE_ALL

    def test_validate_mode_invalid_and_kafka_topic(self):
        # Act
        with pytest.raises(ValueError) as e_info:
            _ = inference_logger.InferenceLogger._validate_mode(
                "invalid", kafka_topic.KafkaTopic()
            )

        # Assert
        assert "is not valid" in str(e_info.value)

    # extract fields from json

    def test_extract_fields_from_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "response"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_logger.InferenceLogger.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["kafka_topic"] == json["kafka_topic_dto"]
        assert kwargs["mode"] == json["inference_logging"]

    def test_extract_fields_from_json_alternative(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["inference_logger"]["get_mode_all_with_kafka_topic"][
            "init_args"
        ]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = inference_logger.InferenceLogger.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["kafka_topic"] == json["kafka_topic"]
        assert kwargs["mode"] == json["mode"]

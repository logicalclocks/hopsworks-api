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
from hopsworks_common import kafka_topic
from hopsworks_common.constants import KAFKA_TOPIC


class TestKafkaTopic:
    # from response json

    def test_from_response_json_with_name_only(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_only"][
            "response"
        ]["kafka_topic_dto"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_kt_from_json = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.from_json"
        )

        # Act
        _ = kafka_topic.KafkaTopic.from_response_json(json_camelized)

        # Assert
        mock_kt_from_json.assert_called_once_with(json)

    def test_from_response_json_with_name_and_config(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_and_config"][
            "response"
        ]["kafka_topic_dto"]
        json_camelized = humps.camelize(json)  # as returned by the backend
        mock_kt_from_json = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.from_json"
        )

        # Act
        _ = kafka_topic.KafkaTopic.from_response_json(json_camelized)

        # Assert
        mock_kt_from_json.assert_called_once_with(json)

    # from json

    def test_from_json_with_name_only(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_only"][
            "response"
        ]["kafka_topic_dto"]
        mock_kt_extract_fields = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.extract_fields_from_json",
            return_value=json,
        )
        mock_kt_init = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.__init__", return_value=None
        )

        # Act
        _ = kafka_topic.KafkaTopic.from_response_json(json)

        # Assert
        mock_kt_extract_fields.assert_called_once_with(json)
        mock_kt_init.assert_called_once_with(**json)

    def test_from_json_with_name_and_config(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_and_config"][
            "response"
        ]["kafka_topic_dto"]
        mock_kt_extract_fields = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.extract_fields_from_json",
            return_value=json,
        )
        mock_kt_init = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic.__init__", return_value=None
        )

        # Act
        _ = kafka_topic.KafkaTopic.from_response_json(json)

        # Assert
        mock_kt_extract_fields.assert_called_once_with(json)
        mock_kt_init.assert_called_once_with(**json)

    # constructor

    def test_constructor_existing_with_name_only(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_only"][
            "response"
        ]["kafka_topic_dto"]
        mock_kt_validate_topic_config = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic._validate_topic_config",
            return_value=(KAFKA_TOPIC.NUM_REPLICAS, KAFKA_TOPIC.NUM_PARTITIONS),
        )

        # Act
        kt = kafka_topic.KafkaTopic(**json)

        # Assert
        assert isinstance(kt, kafka_topic.KafkaTopic)
        assert kt.name == json["name"]
        assert kt.num_replicas == KAFKA_TOPIC.NUM_REPLICAS
        assert kt.num_partitions == KAFKA_TOPIC.NUM_PARTITIONS
        mock_kt_validate_topic_config.assert_called_once_with(json["name"], None, None)

    def test_constructor_existing_with_name_and_config(self, mocker, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_and_config"][
            "response"
        ]["kafka_topic_dto"]
        mock_kt_validate_topic_config = mocker.patch(
            "hopsworks_common.kafka_topic.KafkaTopic._validate_topic_config",
            return_value=(json["num_replicas"], json["num_partitions"]),
        )

        # Act
        kt = kafka_topic.KafkaTopic(**json)

        # Assert
        assert isinstance(kt, kafka_topic.KafkaTopic)
        assert kt.name == json["name"]
        assert kt.num_replicas == json["num_replicas"]
        assert kt.num_partitions == json["num_partitions"]
        mock_kt_validate_topic_config.assert_called_once_with(
            json["name"], json["num_replicas"], json["num_partitions"]
        )

    # validate topic config

    def test_validate_topic_config_existing_with_name_only(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_only"][
            "response"
        ]["kafka_topic_dto"]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            json["name"], None, None
        )

        # Assert
        assert num_repl is None
        assert num_part is None

    def test_validate_topic_config_none(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_none"]["response"][
            "kafka_topic_dto"
        ]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            json["name"], None, None
        )

        # Assert
        assert num_repl is None
        assert num_part is None

    def test_validate_topic_config_none_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_none_with_config"]["response"][
            "kafka_topic_dto"
        ]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            json["name"], json["num_replicas"], json["num_partitions"]
        )

        # Assert
        assert num_repl is None
        assert num_part is None

    def test_validate_topic_config_none_value(self):
        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            None, None, None
        )

        # Assert
        assert num_repl is None
        assert num_part is None

    def test_validate_topic_config_none_value_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_none_with_config"]["response"][
            "kafka_topic_dto"
        ]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            None, json["num_replicas"], json["num_partitions"]
        )

        # Assert
        assert num_repl is None
        assert num_part is None

    def test_validate_topic_config_create(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_create_with_name_only"]["response"][
            "kafka_topic_dto"
        ]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            json["name"], None, None
        )

        # Assert
        assert num_repl == KAFKA_TOPIC.NUM_REPLICAS
        assert num_part == KAFKA_TOPIC.NUM_PARTITIONS

    def test_validate_topic_config_create_with_config(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_create_with_name_and_config"][
            "response"
        ]["kafka_topic_dto"]

        # Act
        num_repl, num_part = kafka_topic.KafkaTopic._validate_topic_config(
            json["name"], json["num_replicas"], json["num_partitions"]
        )

        # Assert
        assert num_repl == json["num_replicas"]
        assert num_part == json["num_partitions"]

    # extract fields from json

    def test_extract_fields_from_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"]["get_existing_with_name_and_config"][
            "response"
        ]["kafka_topic_dto"]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = kafka_topic.KafkaTopic.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["name"] == json["name"]
        assert kwargs["num_replicas"] == json["num_replicas"]
        assert kwargs["num_partitions"] == json["num_partitions"]

    def test_extract_fields_from_json_alternative(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["kafka_topic"][
            "get_existing_with_name_and_config_alternative"
        ]["response"]["kafka_topic_dto"]
        json_copy = copy.deepcopy(json)

        # Act
        kwargs = kafka_topic.KafkaTopic.extract_fields_from_json(json_copy)

        # Assert
        assert kwargs["name"] == json["name"]
        assert kwargs["num_replicas"] == json["num_of_replicas"]
        assert kwargs["num_partitions"] == json["num_of_partitions"]

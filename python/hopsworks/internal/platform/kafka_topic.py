#
#   Copyright 2022 Logical Clocks AB
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

from __future__ import annotations

import json
from typing import Optional

import humps
from hopsworks_common import usage, util
from hopsworks_common.constants import KAFKA_TOPIC
from hopsworks_common.core import kafka_api


class KafkaTopic:
    """Configuration for a Kafka topic."""

    def __init__(
        self,
        name: Optional[str] = KAFKA_TOPIC.CREATE,
        num_of_replicas: Optional[int] = None,
        num_of_partitions: Optional[int] = None,
        schema_name=None,
        schema_version=None,
        schema_content=None,
        shared=None,
        accepted=None,
        type=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        num_replicas: Optional[int] = None,
        num_partitions: Optional[int] = None,
        **kwargs,
    ):
        self._name = name
        if not num_of_replicas:
            num_of_replicas = num_replicas
        if not num_of_partitions:
            num_of_partitions = num_partitions
        self._num_of_replicas, self._num_of_partitions = self._validate_topic_config(
            self._name, num_of_replicas, num_of_partitions
        )
        self._schema_name = schema_name
        self._schema_version = schema_version
        self._schema_content = schema_content
        self._shared = shared
        self._accepted = accepted

        self._kafka_api = kafka_api.KafkaApi()

    def describe(self):
        util.pretty_print(self)

    @classmethod
    def _validate_topic_config(cls, name, num_replicas, num_partitions):
        if name is not None and name != KAFKA_TOPIC.NONE:
            if name == KAFKA_TOPIC.CREATE:
                if num_replicas is None:
                    print(
                        "Setting number of replicas to default value '{}'".format(
                            KAFKA_TOPIC.NUM_REPLICAS
                        )
                    )
                    num_replicas = KAFKA_TOPIC.NUM_REPLICAS
                if num_partitions is None:
                    print(
                        "Setting number of partitions to default value '{}'".format(
                            KAFKA_TOPIC.NUM_PARTITIONS
                        )
                    )
                    num_partitions = KAFKA_TOPIC.NUM_PARTITIONS
        elif name is None or name == KAFKA_TOPIC.NONE:
            num_replicas = None
            num_partitions = None

        return num_replicas, num_partitions

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized:
            return cls.from_json(json_decamelized)
        elif json_decamelized["count"] == 0:
            return []
        else:
            return [cls.from_json(jd) for jd in json_decamelized["items"]]

    @classmethod
    def from_json(cls, json_decamelized):
        return KafkaTopic(**cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        kwargs = {}
        kwargs["name"] = json_decamelized.pop("name")  # required
        kwargs["num_replicas"] = util.extract_field_from_json(
            json_decamelized, ["num_of_replicas", "num_replicas"]
        )
        kwargs["num_partitions"] = util.extract_field_from_json(
            json_decamelized, ["num_of_partitions", "num_partitions"]
        )
        return kwargs

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    @property
    def name(self):
        """Name of the Kafka topic."""
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def replicas(self):
        """Number of replicas of the Kafka topic."""
        return self._num_of_replicas

    @property
    def num_replicas(self):
        """Number of replicas of the Kafka topic."""
        return self._num_of_replicas

    @num_replicas.setter
    def num_replicas(self, num_replicas: int):
        self._num_of_replicas = num_replicas

    @property
    def partitions(self):
        """Number of partitions of the Kafka topic."""
        return self._num_of_partitions

    @property
    def num_partitions(self):
        """Number of partitions of the Kafka topic."""
        return self._num_of_partitions

    @num_partitions.setter
    def topic_num_partitions(self, num_partitions: int):
        self._num_partitions = num_partitions

    @property
    def schema(self):
        """Schema for the topic"""
        return self._kafka_api._get_schema_details(
            self._schema_name, self._schema_version
        )

    @usage.method_logger
    def delete(self):
        """Delete the topic
        !!! danger "Potentially dangerous operation"
            This operation deletes the topic.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        self._kafka_api._delete_topic(self.name)

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "kafkaTopicDTO": {
                "name": self._name,
                "numOfReplicas": self._num_of_replicas,
                "numOfPartitions": self._num_of_partitions,
            }
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"KafkaTopic({self._name!r})"

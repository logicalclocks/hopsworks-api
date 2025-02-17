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
import socket
from typing import Any, Dict, Optional, Union

from hopsworks_common import (
    client,
    constants,
    kafka_schema,
    kafka_topic,
    usage,
)


class KafkaApi:
    @usage.method_logger
    def create_topic(
        self,
        name: str,
        schema: str,
        schema_version: int,
        replicas: int = 1,
        partitions: int = 1,
    ):
        """Create a new kafka topic.

        ```python

        import hopsworks

        project = hopsworks.login()

        kafka_api = project.get_kafka_api()

        kafka_topic = kafka_api.create_topic("my_topic", "my_schema", 1)

        ```
        # Arguments
            name: name of the topic
            schema: subject name of the schema
            schema_version: version of the schema
            replicas: replication factor for the topic
            partitions: partitions for the topic
        # Returns
            `KafkaTopic`: The KafkaTopic object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        path_params = ["project", _client._project_id, "kafka", "topics"]
        data = {
            "name": name,
            "schemaName": schema,
            "schemaVersion": schema_version,
            "numOfReplicas": replicas,
            "numOfPartitions": partitions,
        }

        headers = {"content-type": "application/json"}
        return kafka_topic.KafkaTopic.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(data)
            )
        )

    @usage.method_logger
    def create_schema(self, subject: str, schema: dict):
        """Create a new kafka schema.

        ```python

        import hopsworks

        project = hopsworks.login()

        kafka_api = project.get_kafka_api()

        avro_schema = {
          "type": "record",
          "name": "tutorial",
          "fields": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "data",
              "type": "string"
            }
          ]
        }

        kafka_topic = kafka_api.create_schema("my_schema", avro_schema)

        ```
        # Arguments
            subject: subject name of the schema
            schema: avro schema definition
        # Returns
            `KafkaSchema`: The KafkaSchema object
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()

        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
        ]

        headers = {"content-type": "application/json"}
        schema = kafka_schema.KafkaSchema.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps({"schema": json.dumps(schema)}),
            )
        )
        # TODO: Fix backend, GET request required as POST does not set schema field in the returned payload
        return self.get_schema(schema.subject, schema.version)

    @usage.method_logger
    def get_topic(self, name: str) -> Optional[kafka_topic.KafkaTopic]:
        """Get kafka topic by name.

        # Arguments
            name: name of the topic
        # Returns
            `KafkaTopic`: The KafkaTopic object or `None` if not found
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        topics = self.get_topics()

        for topic in topics:
            if topic.name == name:
                return topic

    @usage.method_logger
    def get_topics(self):
        """Get all kafka topics.

        # Returns
            `List[KafkaTopic]`: List of KafkaTopic objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "kafka", "topics"]

        return kafka_topic.KafkaTopic.from_response_json(
            _client._send_request("GET", path_params)
        )

    def _delete_topic(self, name: str):
        """Delete the topic.
        :param name: name of the topic
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "topics",
            name,
        ]
        _client._send_request("DELETE", path_params)

    def _delete_subject_version(self, subject: str, version: int):
        """Delete the schema.
        :param subject: subject name of the schema
        :type subject: str
        :param version: version of the subject
        :type version: int
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            str(version),
        ]
        _client._send_request("DELETE", path_params)

    @usage.method_logger
    def get_subjects(self):
        """Get all subjects.

        # Returns
            `List[str]`: List of registered subjects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        topics = self.get_topics()

        subjects = set()

        for topic in topics:
            subjects.add(topic.schema.subject)

        return list(subjects)

    @usage.method_logger
    def get_schemas(self, subject: str):
        """Get all schema versions for the subject.

        # Arguments
            subject: subject name
        # Returns
            `List[KafkaSchema]`: List of KafkaSchema objects
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
        ]

        versions = _client._send_request("GET", path_params)

        schemas = []
        for version in versions:
            schemas.append(self._get_schema_details(subject, version))

        return schemas

    @usage.method_logger
    def get_schema(
        self, subject: str, version: int
    ) -> Optional[kafka_schema.KafkaSchema]:
        """Get schema given subject name and version.

        # Arguments
            subject: subject name
            version: version number
        # Returns
            `KafkaSchema`: KafkaSchema object or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        schemas = self.get_schemas(subject)
        for schema in schemas:
            if schema.version == version:
                return schema

    def _get_schema_details(self, subject: str, version: int):
        """Get the schema details.
        :param subject: subject name of the schema
        :type subject: str
        :param version: version of the subject
        :type version: int
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            str(version),
        ]

        return kafka_schema.KafkaSchema.from_response_json(
            _client._send_request("GET", path_params)
        )

    def _get_broker_endpoints(self, externalListeners: bool = False):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "kafka",
            "clusterinfo",
        ]
        query_params = {"external": externalListeners}
        headers = {"content-type": "application/json"}
        return _client._send_request(
            "GET", path_params, query_params=query_params, headers=headers
        )["brokers"]

    def _get_security_protocol(self):
        """
        Gets the security protocol used for communicating with Kafka brokers in a Hopsworks cluster
        Returns:
            the security protocol for communicating with Kafka brokers in a Hopsworks cluster
        """
        return constants.KAFKA_SSL_CONFIG.SSL

    def get_default_config(self, internal_kafka: Optional[bool] = None):
        """Get the configuration to set up a Producer or Consumer for a Kafka broker using confluent-kafka.

        ```python

        import hopsworks

        project = hopsworks.login()

        kafka_api = project.get_kafka_api()

        kafka_conf = kafka_api.get_default_config()

        from confluent_kafka import Producer

        producer = Producer(kafka_conf)

        ```
        # Returns
            `dict`: The kafka configuration
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        _client = client.get_instance()
        config = {
            constants.KAFKA_SSL_CONFIG.SECURITY_PROTOCOL_CONFIG: self._get_security_protocol(),
            constants.KAFKA_SSL_CONFIG.SSL_CA_LOCATION_CONFIG: _client._get_ca_chain_path(),
            constants.KAFKA_SSL_CONFIG.SSL_CERTIFICATE_LOCATION_CONFIG: _client._get_client_cert_path(),
            constants.KAFKA_SSL_CONFIG.SSL_PRIVATE_KEY_LOCATION_CONFIG: _client._get_client_key_path(),
            constants.KAFKA_CONSUMER_CONFIG.CLIENT_ID_CONFIG: socket.gethostname(),
            constants.KAFKA_CONSUMER_CONFIG.GROUP_ID_CONFIG: "my-group-id",
            constants.KAFKA_SSL_CONFIG.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG: "none",
        }

        if internal_kafka is not None:
            config["bootstrap.servers"] = ",".join(
                self._get_broker_endpoints(externalListeners=not internal_kafka)
            )
        else:
            config["bootstrap.servers"] = ",".join(
                self._get_broker_endpoints(externalListeners=_client._is_external())
            )

        return config

    @usage.method_logger
    def get_subject(
        self,
        feature_store_id: int,
        subject: str,
        version: Union[str, int] = "latest",
    ) -> Dict[str, Any]:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "featurestores",
            feature_store_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            version,
        ]
        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

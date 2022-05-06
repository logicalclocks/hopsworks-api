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

from hopsworks import client, kafka_topic, kafka_schema, util, constants
from hopsworks.client.external import Client
from hopsworks.client.exceptions import KafkaException
import os
import json

try:
    from io import BytesIO
    from avro.io import DatumReader, BinaryDecoder
    import avro.schema
except BaseException:
    pass


class KafkaApi:
    def __init__(
        self,
        project_id,
    ):
        self._project_id = project_id

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

        connection = hopsworks.connection()

        project = connection.get_project()

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
            `RestAPIError`: If unable to create the topic
        """
        _client = client.get_instance()

        path_params = ["project", self._project_id, "kafka", "topics"]
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
            ),
            self._project_id,
        )

    def create_schema(self, subject: str, schema: dict):
        """Create a new kafka schema.

        ```python

        import hopsworks

        connection = hopsworks.connection()

        project = connection.get_project()

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
            `RestAPIError`: If unable to create the schema
        """
        _client = client.get_instance()

        path_params = [
            "project",
            self._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
        ]

        headers = {"content-type": "application/json"}
        return kafka_schema.KafkaSchema.from_response_json(
            _client._send_request(
                "POST",
                path_params,
                headers=headers,
                data=json.dumps({"schema": json.dumps(schema)}),
            ),
            self._project_id,
        )

    def get_topic(self, name: str):
        """Get kafka topic by name.

        # Arguments
            name: name of the topic
        # Returns
            `KafkaTopic`: The KafkaTopic object
        # Raises
            `RestAPIError`: If unable to get the topic
        """
        topics = self.get_topics()

        for topic in topics:
            if topic.name == name:
                return topic

        raise KafkaException("No topic named {} could be found".format(name))

    def get_topics(self):
        """Get all kafka topics.

        # Returns
            `List[KafkaTopic]`: List of KafkaTopic objects
        # Raises
            `RestAPIError`: If unable to get the topics
        """
        _client = client.get_instance()
        path_params = ["project", self._project_id, "kafka", "topics"]

        return kafka_topic.KafkaTopic.from_response_json(
            _client._send_request("GET", path_params),
            self._project_id,
        )

    def _delete_topic(self, name: str):
        """Delete the topic.
        :param name: name of the topic
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
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
            self._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            str(version),
        ]
        _client._send_request("DELETE", path_params)

    def get_subjects(self):
        """Get all subjects.

        # Returns
            `List[str]`: List of registered subjects
        # Raises
            `RestAPIError`: If unable to get the subjects
        """
        topics = self.get_topics()

        subjects = set()

        for topic in topics:
            subjects.add(topic.schema.subject)

        return list(subjects)

    def get_schemas(self, subject: str):
        """Get all schema versions for the subject.

        # Arguments
            subject: subject name
        # Returns
            `List[KafkaSchema]`: List of KafkaSchema objects
        # Raises
            `RestAPIError`: If unable to get the schemas
        """
        _client = client.get_instance()
        path_params = [
            "project",
            self._project_id,
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

    def get_schema(self, subject: str, version: int):
        """Get schema given subject name and version.

        # Arguments
            subject: subject name
            version: version number
        # Returns
            `KafkaSchema`: KafkaSchema object
        # Raises
            `RestAPIError`: If unable to get the schema
        """
        schemas = self.get_schemas(subject)
        for schema in schemas:
            if schema.version == version:
                return schema

        raise KafkaException(
            "No schema for subject {} and version {} could be found".format(
                subject, version
            )
        )

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
            self._project_id,
            "kafka",
            "subjects",
            subject,
            "versions",
            str(version),
        ]

        return kafka_schema.KafkaSchema.from_response_json(
            _client._send_request("GET", path_params),
            self._project_id,
        )

    def _get_broker_endpoints(self):
        """
        Get Kafka broker endpoints as a string with broker-endpoints "," separated
        Returns:
            a string with broker endpoints comma-separated
        """
        return os.environ[constants.ENV_VARS.KAFKA_BROKERS].replace("INTERNAL://", "")

    def _get_security_protocol(self):
        """
        Gets the security protocol used for communicating with Kafka brokers in a Hopsworks cluster
        Returns:
            the security protocol for communicating with Kafka brokers in a Hopsworks cluster
        """
        return self.SSL

    def get_default_config(self):
        """
        Gets a default configuration for running secure Kafka on Hopsworks
        Returns:
             dict with config_property --> value
        """

        _client = client.get_instance()

        if type(_client) == Client:
            raise KafkaException(
                "This function is not supported from an external environment."
            )

        default_config = {
            constants.KAFKA_PRODUCER_CONFIG.BOOTSTRAP_SERVERS_CONFIG: self._get_broker_endpoints(),
            constants.KAFKA_SSL_CONFIG.SECURITY_PROTOCOL_CONFIG: self._get_security_protocol(),
            constants.KAFKA_SSL_CONFIG.SSL_CA_LOCATION_CONFIG: util._get_ca_chain_location(),
            constants.KAFKA_SSL_CONFIG.SSL_CERTIFICATE_LOCATION_CONFIG: util._get_client_certificate_location(),
            constants.KAFKA_SSL_CONFIG.SSL_PRIVATE_KEY_LOCATION_CONFIG: util._get_client_key_location(),
            constants.KAFKA_CONSUMER_CONFIG.GROUP_ID_CONFIG: "my-group-id",
        }
        return default_config

    def parse_avro_msg(self, msg: bytes, avro_schema: avro.schema.RecordSchema):
        """
        Parses an avro record using a specified avro schema

        # Arguments
            msg: the avro message to parse
            avro_schema: the avro schema

        # Returns:
             The parsed/decoded message
        """

        reader = DatumReader(avro_schema)
        message_bytes = BytesIO(msg)
        decoder = BinaryDecoder(message_bytes)
        return reader.read(decoder)

    def convert_json_schema_to_avro(self, json_schema):
        """Parses a JSON kafka topic schema into an avro schema

        # Arguments
            json_schema: the json schema to convert
        # Returns
            `avro.schema.RecordSchema`: The Avro record schema
        """
        return avro.schema.parse(json_schema)

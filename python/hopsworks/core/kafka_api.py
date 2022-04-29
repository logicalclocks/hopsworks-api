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

from hopsworks import client, kafka_topic, kafka_schema
from hopsworks.client.external import Client
from hopsworks.client.exceptions import KafkaException
import os
import json
import jks
import base64
import textwrap
from pathlib import Path

try:
    from io import BytesIO
    from avro.io import DatumReader, BinaryDecoder
    import avro.schema
except BaseException:
    pass


class KafkaApi:

    BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers"

    SSL = "SSL"

    SECURITY_PROTOCOL_CONFIG = "security.protocol"
    SSL_CERTIFICATE_LOCATION_CONFIG = "ssl.certificate.location"
    SSL_CA_LOCATION_CONFIG = "ssl.ca.location"
    SSL_PRIVATE_KEY_LOCATION_CONFIG = "ssl.key.location"

    KEYSTORE_SUFFIX = "__kstore.jks"
    TRUSTSTORE_SUFFIX = "__tstore.jks"
    PASSWORD_SUFFIX = "__cert.key"

    K_CERTIFICATE_CONFIG = "k_certificate"
    T_CERTIFICATE_CONFIG = "t_certificate"
    PEM_CLIENT_CERTIFICATE_CONFIG = "client.pem"
    PEM_CLIENT_KEY_CONFIG = "client_key.pem"
    PEM_CA_CHAIN_CERTIFICATE_CONFIG = "ca_chain.pem"
    DOMAIN_CA_TRUSTSTORE = "domain_ca_truststore"
    CRYPTO_MATERIAL_PASSWORD = "material_passwd"

    KAFKA_BROKERS_ENV_VAR = "KAFKA_BROKERS"

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
        return os.environ[self.KAFKA_BROKERS_ENV_VAR].replace("INTERNAL://", "")

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
            self.BOOTSTRAP_SERVERS_CONFIG: self._get_broker_endpoints(),
            self.SECURITY_PROTOCOL_CONFIG: self._get_security_protocol(),
            self.SSL_CA_LOCATION_CONFIG: self._get_ca_chain_location(),
            self.SSL_CERTIFICATE_LOCATION_CONFIG: self._get_client_certificate_location(),
            self.SSL_PRIVATE_KEY_LOCATION_CONFIG: self._get_client_key_location(),
            "group.id": "my-group-id",
        }
        return default_config

    def _get_ca_chain_location(self):
        """
        Get location of chain of CA certificates (PEM format) that are required to validate the
        private key certificate of the client
        used for 2-way TLS authentication, for example with Kafka cluster
        Returns:
             string path to ca chain of certificate
        """
        ca_chain_path = Path(self.PEM_CA_CHAIN_CERTIFICATE_CONFIG)
        if not ca_chain_path.exists():
            self._write_pems()
        return str(ca_chain_path)

    def _get_client_certificate_location(self):
        """
        Get location of client certificate (PEM format) for the private key signed by trusted CA
        used for 2-way TLS authentication, for example with Kafka cluster
        Returns:
            string path to client certificate in PEM format
        """
        certificate_path = Path(self.PEM_CLIENT_CERTIFICATE_CONFIG)
        if not certificate_path.exists():
            self._write_pems()
        return str(certificate_path)

    def _get_client_key_location(self):
        """
        Get location of client private key (PEM format)
        used for for 2-way TLS authentication, for example with Kafka cluster
        Returns:
            string path to client private key in PEM format
        """
        # Convert JKS to PEMs if they don't exists already
        key_path = Path(self.PEM_CLIENT_KEY_CONFIG)
        if not key_path.exists():
            self._write_pems()
        return str(key_path)

    def _write_pems(self):
        """
        Converts JKS keystore file into PEM to be compatible with Python libraries
        """
        t_jks_path = self._get_trust_store()
        k_jks_path = self._get_key_store()

        client_certificate_path = Path(self.PEM_CLIENT_CERTIFICATE_CONFIG)
        client_key_path = Path(self.PEM_CLIENT_KEY_CONFIG)
        ca_chain_path = Path(self.PEM_CA_CHAIN_CERTIFICATE_CONFIG)

        self._write_pem(
            k_jks_path,
            t_jks_path,
            self._get_key_store_pwd(),
            client_certificate_path,
            client_key_path,
            ca_chain_path,
        )

    def _write_pem(
        self,
        jks_key_store_path,
        jks_trust_store_path,
        keystore_pw,
        client_key_cert_path,
        client_key_path,
        ca_cert_path,
    ):
        """
        Converts the JKS keystore, JKS truststore, and the root ca.pem
        client certificate, client key, and ca certificate
        Args:
        :jks_key_store_path: path to the JKS keystore
        :jks_trust_store_path: path to the JKS truststore
        :keystore_pw: path to file with passphrase for the keystores
        :client_key_cert_path: path to write the client's certificate for its private key in PEM format
        :client_key_path: path to write the client's private key in PEM format
        :ca_cert_path: path to write the chain of CA certificates required to validate certificates
        """
        keystore_key_cert, keystore_key, keystore_ca_cert = self._convert_jks_to_pem(
            jks_key_store_path, keystore_pw
        )
        (
            truststore_key_cert,
            truststore_key,
            truststore_ca_cert,
        ) = self._convert_jks_to_pem(jks_trust_store_path, keystore_pw)
        with client_key_cert_path.open("w") as f:
            f.write(keystore_key_cert)
        with client_key_path.open("w") as f:
            f.write(keystore_key)
        with ca_cert_path.open("w") as f:
            f.write(keystore_ca_cert + truststore_ca_cert)

    def _convert_jks_to_pem(self, jks_path, keystore_pw):
        """
        Converts a keystore JKS that contains client private key,
         client certificate and CA certificate that was used to
         sign the certificate, to three PEM-format strings.
        Args:
        :jks_path: path to the JKS file
        :pw: password for decrypting the JKS file
        Returns:
             strings: (client_cert, client_key, ca_cert)
        """
        # load the keystore and decrypt it with password
        ks = jks.KeyStore.load(jks_path, keystore_pw, try_decrypt_keys=True)
        private_keys_certs = ""
        private_keys = ""
        ca_certs = ""

        # Convert private keys and their certificates into PEM format and append to string
        for alias, pk in ks.private_keys.items():
            if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
                private_keys = private_keys + self._bytes_to_pem_str(
                    pk.pkey, "RSA PRIVATE KEY"
                )
            else:
                private_keys = private_keys + self._bytes_to_pem_str(
                    pk.pkey_pkcs8, "PRIVATE KEY"
                )
            for c in pk.cert_chain:
                # c[0] contains type of cert, i.e X.509
                private_keys_certs = private_keys_certs + self._bytes_to_pem_str(
                    c[1], "CERTIFICATE"
                )

        # Convert CA Certificates into PEM format and append to string
        for alias, c in ks.certs.items():
            ca_certs = ca_certs + self._bytes_to_pem_str(c.cert, "CERTIFICATE")
        return private_keys_certs, private_keys, ca_certs

    def _bytes_to_pem_str(self, der_bytes, pem_type):
        """
        Utility function for creating PEM files
        Args:
            der_bytes: DER encoded bytes
            pem_type: type of PEM, e.g Certificate, Private key, or RSA private key
        Returns:
            PEM String for a DER-encoded certificate or private key
        """
        pem_str = ""
        pem_str = pem_str + "-----BEGIN {}-----".format(pem_type) + "\n"
        pem_str = (
            pem_str
            + "\r\n".join(
                textwrap.wrap(base64.b64encode(der_bytes).decode("ascii"), 64)
            )
            + "\n"
        )
        pem_str = pem_str + "-----END {}-----".format(pem_type) + "\n"
        return pem_str

    def _get_key_store_path(self):
        """
        Get keystore path
        Returns:
            keystore path
        """
        k_certificate = Path(self.K_CERTIFICATE_CONFIG)
        if k_certificate.exists():
            return k_certificate
        else:
            username = os.environ["HADOOP_USER_NAME"]
            material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
            return material_directory.joinpath(username + self.KEYSTORE_SUFFIX)

    def _get_key_store(self):
        return str(self._get_key_store_path())

    def _get_trust_store_path(self):
        """
        Get truststore location
        Returns:
             truststore location
        """
        t_certificate = Path(self.T_CERTIFICATE_CONFIG)
        if t_certificate.exists():
            return str(t_certificate)
        else:
            username = os.environ["HADOOP_USER_NAME"]
            material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
            return str(material_directory.joinpath(username + self.TRUSTSTORE_SUFFIX))

    def _get_trust_store(self):
        return str(self._get_trust_store_path())

    def _get_trust_store_pwd(self):
        """
        Get truststore password
        Returns:
             truststore password
        """
        return self._get_cert_pw()

    def _get_cert_pw(self):
        """
        Get keystore password from local container
        Returns:
            Certificate password
        """
        pwd_path = Path(self.CRYPTO_MATERIAL_PASSWORD)
        if not pwd_path.exists():
            username = os.environ["HADOOP_USER_NAME"]
            material_directory = Path(os.environ["MATERIAL_DIRECTORY"])
            pwd_path = material_directory.joinpath(username + self.PASSWORD_SUFFIX)

        with pwd_path.open() as f:
            return f.read()

    def _get_key_store_pwd(self):
        """
        Get keystore password
        Returns:
             keystore password
        """
        return self._get_cert_pw()

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

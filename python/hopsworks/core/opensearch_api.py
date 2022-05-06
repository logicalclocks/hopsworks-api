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

from hopsworks import client, constants, util
from hopsworks.client.external import Client
import os
from hopsworks.client.exceptions import OpenSearchException


class OpenSearchApi:
    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name

        _client = client.get_instance()

        if type(_client) == Client:
            raise OpenSearchException(
                "This API is not supported from an external environment."
            )

    def _get_elasticsearch_url(self):
        return os.environ[constants.ENV_VARS.ELASTIC_ENDPOINT_ENV_VAR]

    def get_elasticsearch_index(self, index):
        """
        Get the valid elasticsearch index for later use. This helper method prefix the index name with the project name.

        Args:
            :index: the elasticsearch index to interact with.

        Returns:
            A valid elasticsearch index name.
        """
        return (self._project_name + "_" + index).lower()

    def get_default_py_config(self):
        """
        Get the required elasticsearch configuration to setup a connection using opensearch connector.

        Args:
            :index: the elasticsearch index to interact with.

        Returns:
            A dictionary with required configuration.
        """
        host = self._get_elasticsearch_url().split(":")[0]
        return {
            constants.OPENSEARCH_CONFIG.HOSTS: [{"host": host, "port": 9200}],
            constants.OPENSEARCH_CONFIG.HTTP_COMPRESS: False,
            constants.OPENSEARCH_CONFIG.HEADERS: {
                "Authorization": self.get_authorization_token()
            },
            constants.OPENSEARCH_CONFIG.USE_SSL: True,
            constants.OPENSEARCH_CONFIG.VERIFY_CERTS: True,
            constants.OPENSEARCH_CONFIG.SSL_ASSERT_HOSTNAME: False,
            constants.OPENSEARCH_CONFIG.CA_CERTS: util.get_ca_chain_location(),
        }

    def get_default_spark_config(self, index):
        """
        Get the required elasticsearch configuration to setup a connection using spark connector.

        Args:
            :index: the elasticsearch index to interact with.

        Returns:
            A dictionary with required configuration.
        """
        config = {
            constants.OPENSEARCH_CONFIG.SSL_CONFIG: "true",
            constants.OPENSEARCH_CONFIG.NODES: self._get_elasticsearch_url(),
            constants.OPENSEARCH_CONFIG.NODES_WAN_ONLY: "true",
            constants.OPENSEARCH_CONFIG.SSL_KEYSTORE_LOCATION: util.get_key_store(),
            constants.OPENSEARCH_CONFIG.SSL_KEYSTORE_PASSWORD: util.get_key_store_pwd(),
            constants.OPENSEARCH_CONFIG.SSL_TRUSTSTORE_LOCATION: util.get_trust_store(),
            constants.OPENSEARCH_CONFIG.SSL_TRUSTSTORE_PASSWORD: util.get_trust_store_pwd(),
            constants.OPENSEARCH_CONFIG.HTTP_AUTHORIZATION: self.get_authorization_token(),
            constants.OPENSEARCH_CONFIG.INDEX: self.get_elasticsearch_index(index),
        }
        return config

    def get_elasticsearch_config(self, index):
        """
        Get the required elasticsearch configuration to setup a connection using spark connector.

        Args:
            :index: the elasticsearch index to interact with.

        Returns:
            A dictionary with required configuration.
        """
        config = {
            constants.OPENSEARCH_CONFIG.SSL_CONFIG: "true",
            constants.OPENSEARCH_CONFIG.NODES: self._get_elasticsearch_url(),
            constants.OPENSEARCH_CONFIG.NODES_WAN_ONLY: "true",
            constants.OPENSEARCH_CONFIG.SSL_KEYSTORE_LOCATION: util.get_key_store(),
            constants.OPENSEARCH_CONFIG.SSL_KEYSTORE_PASSWORD: util.get_key_store_pwd(),
            constants.OPENSEARCH_CONFIG.SSL_TRUSTSTORE_LOCATION: util.get_trust_store(),
            constants.OPENSEARCH_CONFIG.SSL_TRUSTSTORE_PASSWORD: util.get_trust_store_pwd(),
            constants.OPENSEARCH_CONFIG.HTTP_AUTHORIZATION: self.get_authorization_token(),
            constants.OPENSEARCH_CONFIG.INDEX: self.get_elasticsearch_index(index),
        }
        return config

    def get_authorization_token(self):

        """Get configuration for the specific job type.

        # Arguments
            type: Type of the job. Currently, supported types include: SPARK, PYSPARK, PYTHON, DOCKER, FLINK.
        # Returns
            `dict`: Default job configuration
        # Raises
            `RestAPIError`: If unable to get the job configuration
        """

        _client = client.get_instance()
        path_params = ["elastic", "jwt", self._project_id]

        headers = {"content-type": "application/json"}
        return _client._send_request("GET", path_params, headers=headers)

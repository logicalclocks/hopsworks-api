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
import os


class OpenSearchApi:

    def __init__(
        self,
        project_id,
        project_name,
    ):
        self._project_id = project_id
        self._project_name = project_name


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
            constants.OPENSEARCH_CONFIG.INDEX: self.get_elasticsearch_index(index)
        }
        return config


    def get_authorization_token(self):
        """
        Get the authorization token to interact with elasticsearch.

        Args:

        Returns:
            The authorization token to be used in http header.
        """
        headers = {constants.HTTP_CONFIG.HTTP_CONTENT_TYPE: constants.HTTP_CONFIG.HTTP_APPLICATION_JSON}
        method = constants.HTTP_CONFIG.HTTP_GET
        resource_url = constants.DELIMITERS.SLASH_DELIMITER + \
                       constants.REST_CONFIG.HOPSWORKS_REST_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                       constants.REST_CONFIG.HOPSWORKS_ELASTIC_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                       constants.REST_CONFIG.HOPSWORKS_ELASTIC_JWT_RESOURCE + constants.DELIMITERS.SLASH_DELIMITER + \
                       hdfs.project_id()
        response = util.send_request(method, resource_url, headers=headers)
        response_object = response.json()
        if response.status_code >= 400:
            error_code, error_msg, user_msg = util._parse_rest_error(response_object)
            raise RestAPIError("Could not get authorization token for elastic (url: {}), server response: \n " \
                               "HTTP code: {}, HTTP reason: {}, error code: {}, error msg: {}, user msg: {}".format(
                resource_url, response.status_code, response.reason, error_code, error_msg, user_msg))

        return "Bearer " + response_object["token"]
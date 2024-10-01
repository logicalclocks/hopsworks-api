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

import os

import requests
from hopsworks_common.client import auth, exceptions
from hopsworks_common.client.istio import base as istio


class Client(istio.Client):
    REQUESTS_VERIFY = "REQUESTS_VERIFY"
    PROJECT_ID = "HOPSWORKS_PROJECT_ID"
    PROJECT_NAME = "HOPSWORKS_PROJECT_NAME"
    HADOOP_USER_NAME = "HADOOP_USER_NAME"
    HDFS_USER = "HDFS_USER"

    DOMAIN_CA_TRUSTSTORE_PEM = "DOMAIN_CA_TRUSTSTORE_PEM"
    MATERIAL_DIRECTORY = "MATERIAL_DIRECTORY"
    T_CERTIFICATE = "t_certificate"
    K_CERTIFICATE = "k_certificate"
    TRUSTSTORE_SUFFIX = "__tstore.jks"
    KEYSTORE_SUFFIX = "__kstore.jks"
    PEM_CA_CHAIN = "ca_chain.pem"
    CERT_KEY_SUFFIX = "__cert.key"
    MATERIAL_PWD = "material_passwd"
    SECRETS_DIR = "SECRETS_DIR"

    def __init__(self, host, port):
        """Initializes a client being run from a job/notebook directly on Hopsworks."""
        self._host = host
        self._port = port
        self._base_url = "http://" + self._host + ":" + str(self._port)

        trust_store_path = self._get_ca_chain_path()
        hostname_verification = (
            os.environ[self.REQUESTS_VERIFY]
            if self.REQUESTS_VERIFY in os.environ
            else "true"
        )
        self._project_id = os.environ[self.PROJECT_ID]
        self._project_name = self._project_name()
        self._auth = auth.ApiKeyAuth(self._get_serving_api_key())
        self._verify = self._get_verify(hostname_verification, trust_store_path)
        self._session = requests.session()

        self._connected = True

    def _project_name(self):
        try:
            return os.environ[self.PROJECT_NAME]
        except KeyError:
            pass

        hops_user = self._project_user()
        hops_user_split = hops_user.split(
            "__"
        )  # project users have username project__user
        project = hops_user_split[0]
        return project

    def _project_user(self):
        try:
            hops_user = os.environ[self.HADOOP_USER_NAME]
        except KeyError:
            hops_user = os.environ[self.HDFS_USER]
        return hops_user

    def _get_ca_chain_path(self) -> str:
        return os.path.join("/tmp", self.PEM_CA_CHAIN)

    def _get_serving_api_key(self):
        """Retrieve serving API key from environment variable."""
        if self.SERVING_API_KEY not in os.environ:
            raise exceptions.InternalClientError("Serving API key not found")
        return os.environ[self.SERVING_API_KEY]

    def replace_public_host(self, url):
        """replace hostname to public hostname set in HOPSWORKS_PUBLIC_HOST"""
        ui_url = url._replace(netloc=os.environ[self.HOPSWORKS_PUBLIC_HOST])
        return ui_url

    def _is_external(self):
        return False

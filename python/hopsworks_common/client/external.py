#
#   Copyright 2020 Logical Clocks AB
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

import base64
import logging
import os

import hopsworks_common.client
import requests
from hopsworks_common.client import auth, base, exceptions
from hopsworks_common.client.exceptions import FeatureStoreException


try:
    from pyspark.sql import SparkSession
except ImportError:
    pass


_logger = logging.getLogger(__name__)


class Client(base.Client):
    def __init__(
        self,
        host,
        port,
        project,
        engine,
        hostname_verification,
        trust_store_path,
        cert_folder,
        api_key_file,
        api_key_value,
    ):
        """Initializes a client in an external environment such as AWS Sagemaker."""
        _logger.info("Initializing external client")
        if not host:
            raise exceptions.ExternalClientError("host")

        self._host = host
        self._port = port
        self._base_url = "https://" + self._host + ":" + str(self._port)
        _logger.info("Base URL: %s", self._base_url)

        if api_key_value is not None:
            _logger.debug("Using provided API key value")
            api_key = api_key_value
        else:
            _logger.debug(f"Reading api key from {api_key_file}")
            with open(api_key_file) as f:
                api_key = f.readline().strip()

        _logger.debug("Using api key to setup header authentification")
        self._auth = auth.ApiKeyAuth(api_key)

        _logger.debug("Setting up requests session")
        self._session = requests.session()
        self._connected = True

        self._verify = self._get_verify(hostname_verification, trust_store_path)
        self._hostname_verification = hostname_verification
        self._hopsworks_ca_trust_store_path = trust_store_path
        _logger.debug("Verify: %s", self._verify)

        self._cert_key = None
        self._cert_folder_base = cert_folder
        self._cert_folder = None

        self._engine = engine

        if not project:
            # This should only happen due to the way of work of and in hopsworks.login:
            # It first initalizes a client, and then uses it to figure out which project to use.
            self._project_name = None
            return

        self.provide_project(project)

    def provide_project(self, project):
        self._project_name = project
        _logger.debug("Project name: %s", self._project_name)

        project_info = self._get_project_info(project)
        self._project_id = str(project_info["projectId"])
        _logger.debug("Setting Project ID: %s", self._project_id)

        if self._engine == "python":
            self.download_certs()

        elif self._engine == "spark":
            # When using the Spark engine with metastore connection, the certificates
            # are needed when the application starts (before user code is run)
            # So in this case, we can't materialize the certificates on the fly.
            _logger.debug("Running in Spark environment, initializing Spark session")
            _spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()

            self._validate_spark_configuration(_spark_session)
            with open(
                _spark_session.conf.get("spark.hadoop.hops.ssl.keystores.passwd.name"),
                "r",
            ) as f:
                self._cert_key = f.read()

            self._trust_store_path = _spark_session.conf.get(
                "spark.hadoop.hops.ssl.trustore.name"
            )
            self._key_store_path = _spark_session.conf.get(
                "spark.hadoop.hops.ssl.keystore.name"
            )

        elif self._engine == "spark-no-metastore":
            _logger.debug(
                "Running in Spark environment with no metastore, initializing Spark session"
            )
            _spark_session = SparkSession.builder.getOrCreate()
            self.download_certs()

            # Set credentials location in the Spark configuration
            # Set other options in the Spark configuration
            configuration_dict = {
                "hops.ssl.trustore.name": self._trust_store_path,
                "hops.ssl.keystore.name": self._key_store_path,
                "hops.ssl.keystores.passwd.name": self._cert_key_path,
                "fs.permissions.umask-mode": "0002",
                "fs.hopsfs.impl": "io.hops.hopsfs.client.HopsFileSystem",
                "hops.rpc.socket.factory.class.default": "io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory",
                "client.rpc.ssl.enabled.protocol": "TLSv1.2",
                "hops.ssl.hostname.verifier": "ALLOW_ALL",
                "hops.ipc.server.ssl.enabled": "true",
            }

            for conf_key, conf_value in configuration_dict.items():
                _spark_session._jsc.hadoopConfiguration().set(conf_key, conf_value)

        elif self._engine == "spark-delta":
            _logger.debug(
                "Running in Spark environment with no metastore and hopsfs, initializing Spark session"
            )
            self.download_certs()
            _spark_session = (
                SparkSession.builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .getOrCreate()
            )

        hopsworks_common.client.get_connection()._provide_project()

    def download_certs(self):
        res = self._materialize_certs()
        self._write_pem_file(res["caChain"], self._get_ca_chain_path())
        self._write_pem_file(res["clientCert"], self._get_client_cert_path())
        self._write_pem_file(res["clientKey"], self._get_client_key_path())
        return res

    def _materialize_certs(self):
        self._cert_folder = os.path.join(
            self._cert_folder_base, self._host, self._project_name
        )
        self._trust_store_path = os.path.join(self._cert_folder, "trustStore.jks")
        self._key_store_path = os.path.join(self._cert_folder, "keyStore.jks")

        if os.path.exists(self._cert_folder):
            _logger.debug(
                f"Running in Python environment, reading certificates from certificates folder {self._cert_folder_base}"
            )
            _logger.debug("Found certificates: %s", os.listdir(self._cert_folder_base))
        else:
            _logger.debug(
                f"Running in Python environment, creating certificates folder {self._cert_folder_base}"
            )
            os.makedirs(self._cert_folder, exist_ok=True)

        credentials = self._get_credentials(self._project_id)
        self._write_b64_cert_to_bytes(
            str(credentials["kStore"]),
            path=self._get_jks_key_store_path(),
        )
        self._write_b64_cert_to_bytes(
            str(credentials["tStore"]),
            path=self._get_jks_trust_store_path(),
        )

        self._cert_key = str(credentials["password"])
        self._cert_key_path = os.path.join(self._cert_folder, "material_passwd")
        with open(self._cert_key_path, "w") as f:
            f.write(str(credentials["password"]))

        # Return the credentials object for the Python engine to materialize the pem files.
        return credentials

    def _validate_spark_configuration(self, _spark_session):
        exception_text = "Spark is misconfigured for communication with Hopsworks, missing or invalid property: "

        configuration_dict = {
            "spark.hadoop.hops.ssl.trustore.name": None,
            "spark.hadoop.hops.rpc.socket.factory.class.default": "io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.hadoop.hops.ssl.hostname.verifier": "ALLOW_ALL",
            "spark.hadoop.hops.ssl.keystore.name": None,
            "spark.hadoop.fs.hopsfs.impl": "io.hops.hopsfs.client.HopsFileSystem",
            "spark.hadoop.hops.ssl.keystores.passwd.name": None,
            "spark.hadoop.hops.ipc.server.ssl.enabled": "true",
            "spark.hadoop.client.rpc.ssl.enabled.protocol": "TLSv1.2",
            "spark.hadoop.hive.metastore.uris": None,
            "spark.sql.hive.metastore.jars": None,
        }
        _logger.debug("Configuration dict: %s", configuration_dict)

        for key, value in configuration_dict.items():
            _logger.debug("Validating key: %s", key)
            if not (
                _spark_session.conf.get(key, "not_found") != "not_found"
                and (value is None or _spark_session.conf.get(key, None) == value)
            ):
                raise FeatureStoreException(exception_text + key)

    def _close(self):
        """Closes a client and deletes certificates."""
        _logger.info("Closing external client and cleaning up certificates.")
        self._connected = False
        if self._cert_folder is None:
            _logger.debug("No certificates to clean up.")
            # On external Spark clients (Databricks, Spark Cluster),
            # certificates need to be provided before the Spark application starts.
            return

        # Clean up only on AWS
        _logger.debug("Cleaning up certificates. AWS only.")
        self._cleanup_file(self._get_jks_key_store_path())
        self._cleanup_file(self._get_jks_trust_store_path())
        self._cleanup_file(os.path.join(self._cert_folder, "material_passwd"))
        self._cleanup_file(self._get_ca_chain_path())
        self._cleanup_file(self._get_client_cert_path())
        self._cleanup_file(self._get_client_key_path())

        try:
            # delete project level
            os.rmdir(self._cert_folder)
            # delete host level
            os.rmdir(os.path.dirname(self._cert_folder))
            # on AWS base dir will be empty, and can be deleted otherwise raises OSError
            os.rmdir(self._cert_folder_base)
        except OSError:
            pass

        self._cert_folder = None

    def _get_jks_trust_store_path(self):
        _logger.debug("Getting trust store path: %s", self._trust_store_path)
        return self._trust_store_path

    def _get_jks_key_store_path(self):
        _logger.debug("Getting key store path: %s", self._key_store_path)
        return self._key_store_path

    def _get_ca_chain_path(self) -> str:
        path = os.path.join(
            self._cert_folder_base, self._host, self._project_name, "ca_chain.pem"
        )
        _logger.debug(f"Getting ca chain path {path}")
        return path

    def _get_client_cert_path(self) -> str:
        path = os.path.join(
            self._cert_folder_base, self._host, self._project_name, "client_cert.pem"
        )
        _logger.debug(f"Getting client cert path {path}")
        return path

    def _get_client_key_path(self) -> str:
        path = os.path.join(
            self._cert_folder_base, self._host, self._project_name, "client_key.pem"
        )
        _logger.debug(f"Getting client key path {path}")
        return path

    def _get_project_info(self, project_name):
        """Makes a REST call to hopsworks to get all metadata of a project for the provided project.

        :param project_name: the name of the project
        :type project_name: str
        :return: JSON response with project info
        :rtype: dict
        """
        _logger.debug("Getting project info for project: %s", project_name)
        return self._send_request("GET", ["project", "getProjectInfo", project_name])

    def _write_b64_cert_to_bytes(self, b64_string, path):
        """Converts b64 encoded certificate to bytes file .

        :param b64_string:  b64 encoded string of certificate
        :type b64_string: str
        :param path: path where file is saved, including file name. e.g. /path/key-store.jks
        :type path: str
        """
        _logger.debug(f"Writing b64 encoded certificate to {path}")
        with open(path, "wb") as f:
            cert_b64 = base64.b64decode(b64_string)
            f.write(cert_b64)

    def _cleanup_file(self, file_path):
        """Removes local files with `file_path`."""
        _logger.debug(f"Cleaning up file {file_path}")
        try:
            os.remove(file_path)
        except OSError:
            pass

    def replace_public_host(self, url):
        """no need to replace as we are already in external client"""
        return url

    def _is_external(self):
        return True

    @property
    def host(self):
        return self._host
